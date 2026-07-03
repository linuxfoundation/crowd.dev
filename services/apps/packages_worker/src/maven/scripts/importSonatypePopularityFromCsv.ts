/**
 * One-shot import of the Sonatype popularity signal into the osspckgs `packages`
 * table.
 *
 * Sonatype could not deliver raw Maven download counts, so instead they provide a
 * popularity score derived from their SCA telemetry: normalized 0-100 per
 * ecosystem, tiered P0-P3. First delivery is Maven P0 only (top ~500 components).
 *
 * Safe to run multiple times — every write goes through upsertSonatypePopularity,
 * keyed on the composite identity (ecosystem, namespace, name). Rows not yet in
 * `packages` are inserted with ingestion_source='sonatype' (deps.dev / Maven
 * enrichment backfills the rest later); existing rows get only their sonatype_*
 * fields refreshed and keep their original ingestion_source.
 *
 * Expected CSV header (exact): format,namespace,name,component_key,popularity_score,rank,tier
 *   - namespace = Maven groupId, name = Maven artifactId
 *   - component_key = groupId:artifactId (used only as a sanity check)
 *
 * Flags:
 *   --dry-run                 Parse + validate, print what would happen, no DB writes
 *   --snapshot-date=YYYY-MM-DD  Snapshot timestamp; derived from the filename if omitted
 *
 * Usage:
 *   tsx src/maven/scripts/importSonatypePopularityFromCsv.ts <input.csv> [--snapshot-date=YYYY-MM-DD] [--dry-run]
 */
import * as fs from 'fs'
import * as path from 'path'

import { upsertSonatypePopularity } from '@crowd/data-access-layer/src/osspckgs/packages'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { getPackagesDbConnection } from '../../db'

import { parseCsv } from './csv'

const EXPECTED_HEADER = [
  'format',
  'namespace',
  'name',
  'component_key',
  'popularity_score',
  'rank',
  'tier',
]
const VALID_TIERS = new Set(['P0', 'P1', 'P2', 'P3'])

// packages.purl is stored version-stripped for Maven (one row per groupId:artifactId).
// Mirror that exact format for insert-if-missing rows so a later deps.dev / Maven
// enrichment upsert (keyed on purl) resolves to the same row instead of duplicating it.
function buildMavenPurl(groupId: string, artifactId: string): string {
  return `pkg:maven/${groupId}/${artifactId}`
}

function resolveSnapshotDate(args: string[], inputPath: string): Date {
  const flag = args.find((a) => a.startsWith('--snapshot-date='))
  const explicit = flag?.split('=')[1]
  const raw = explicit ?? path.basename(inputPath).match(/(\d{4}-\d{2}-\d{2})/)?.[1]

  if (!raw) {
    throw new Error(
      'Could not determine snapshot date: pass --snapshot-date=YYYY-MM-DD or include a YYYY-MM-DD in the filename',
    )
  }
  const date = new Date(`${raw}T00:00:00Z`)
  if (isNaN(date.getTime())) {
    throw new Error(`Invalid snapshot date: "${raw}" (expected YYYY-MM-DD)`)
  }
  return date
}

type SonatypeRow = {
  namespace: string
  name: string
  score: number
  rank: number
  tier: string
}

// Parse + validate one CSV record. Returns null for rows to skip (non-maven or
// malformed), with the reason logged so nothing is dropped silently.
function parseRow(r: Record<string, string>, lineNo: number): SonatypeRow | null {
  if (r.format !== 'maven') return null // defensive: first delivery is maven-only

  const namespace = r.namespace
  const name = r.name
  const score = Number(r.popularity_score)
  const rank = Number(r.rank)
  const tier = r.tier

  if (!namespace || !name) {
    console.warn(`  line ${lineNo}: missing namespace/name — skipped`)
    return null
  }
  if (!Number.isFinite(score) || score < 0 || score > 100) {
    console.warn(`  line ${lineNo}: invalid popularity_score "${r.popularity_score}" — skipped`)
    return null
  }
  if (!Number.isInteger(rank) || rank <= 0) {
    console.warn(`  line ${lineNo}: invalid rank "${r.rank}" — skipped`)
    return null
  }
  if (!VALID_TIERS.has(tier)) {
    console.warn(`  line ${lineNo}: invalid tier "${tier}" — skipped`)
    return null
  }
  // component_key is groupId:artifactId — cross-check against namespace/name.
  if (r.component_key && r.component_key !== `${namespace}:${name}`) {
    console.warn(
      `  line ${lineNo}: component_key "${r.component_key}" != "${namespace}:${name}" — skipped`,
    )
    return null
  }

  return { namespace, name, score, rank, tier }
}

async function main() {
  const args = process.argv.slice(2)
  const dryRun = args.includes('--dry-run')
  const inputPath = args.find((a) => !a.startsWith('--'))

  if (!inputPath) {
    console.error(
      'Usage: tsx src/maven/scripts/importSonatypePopularityFromCsv.ts <input.csv> [--snapshot-date=YYYY-MM-DD] [--dry-run]',
    )
    process.exit(1)
  }

  const snapshotAt = resolveSnapshotDate(args, inputPath)

  console.log(`Input:        ${inputPath}`)
  console.log(`Snapshot:     ${snapshotAt.toISOString()}`)
  console.log(`Mode:         ${dryRun ? 'DRY RUN (no DB writes)' : 'LIVE'}`)
  console.log()

  const content = fs.readFileSync(inputPath, 'utf-8')
  const records = parseCsv(content)

  if (records.length === 0) {
    throw new Error('CSV is empty (no data rows)')
  }

  // Validate the header exactly — order-independent, no missing/extra columns.
  const header = Object.keys(records[0])
  const missing = EXPECTED_HEADER.filter((h) => !header.includes(h))
  const extra = header.filter((h) => !EXPECTED_HEADER.includes(h))
  if (missing.length > 0 || extra.length > 0) {
    throw new Error(
      `Unexpected CSV header.\n  expected: ${EXPECTED_HEADER.join(', ')}\n  got:      ${header.join(', ')}` +
        (missing.length ? `\n  missing:  ${missing.join(', ')}` : '') +
        (extra.length ? `\n  extra:    ${extra.join(', ')}` : ''),
    )
  }

  let skipped = 0
  const rows: SonatypeRow[] = []
  records.forEach((r, i) => {
    const parsed = parseRow(r, i + 2) // +2: 1-based, and header is line 1
    if (parsed) rows.push(parsed)
    else skipped++
  })

  console.log(`Parsed ${records.length} data rows — ${rows.length} valid maven, ${skipped} skipped`)

  if (dryRun) {
    rows.slice(0, 10).forEach((r) => {
      console.log(`  ${r.namespace}:${r.name}  score=${r.score} rank=${r.rank} tier=${r.tier}`)
    })
    if (rows.length > 10) console.log(`  ... (${rows.length - 10} more)`)
    console.log('\nDry run complete — no changes made.')
    return
  }

  const conn = await getPackagesDbConnection()

  const insertedRows: SonatypeRow[] = []
  let updated = 0

  // Single transaction: the dataset is small (hundreds of rows) and a one-shot
  // import should be all-or-nothing.
  await conn.tx(async (t) => {
    const tqx = pgpQx(t)
    for (const r of rows) {
      const { inserted: wasInserted } = await upsertSonatypePopularity(tqx, {
        purl: buildMavenPurl(r.namespace, r.name),
        ecosystem: 'maven',
        namespace: r.namespace,
        name: r.name,
        sonatypePopularityScore: r.score,
        sonatypeRank: r.rank,
        sonatypeTier: r.tier,
        sonatypeSnapshotAt: snapshotAt,
      })
      if (wasInserted) insertedRows.push(r)
      else updated++
    }
  })

  await (conn as unknown as { $pool: { end: () => Promise<void> } }).$pool.end()

  console.log(`\n✓ Done.`)
  console.log(`  Inserted (new packages): ${insertedRows.length}`)
  console.log(`  Updated (existing):      ${updated}`)
  console.log(`  Skipped (non-maven / invalid): ${skipped}`)

  // List the newly-inserted packages — these are the Sonatype rows that were not
  // in `packages` yet (deps.dev hasn't backfilled them). Small set, worth an audit
  // trail so the overlap can be reported without re-querying the DB.
  if (insertedRows.length > 0) {
    console.log(`\n  New packages inserted (ingestion_source='sonatype'):`)
    insertedRows
      .sort((a, b) => a.rank - b.rank)
      .forEach((r) =>
        console.log(`    rank ${r.rank}\t${r.tier}\tscore ${r.score}\t${r.namespace}:${r.name}`),
      )
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
