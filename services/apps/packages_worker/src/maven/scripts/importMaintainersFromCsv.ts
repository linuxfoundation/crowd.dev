/**
 * Imports maintainer data from the CSV produced by fetchMavenMaintainers.ts
 * into the osspckgs database (maintainers + package_maintainers tables).
 *
 * Safe to run multiple times — all writes are INSERT ... ON CONFLICT DO NOTHING.
 * Existing maintainer data from POM extraction is never deleted.
 *
 * Generates a rollback.sql file next to the input CSV so the import can be
 * fully reversed: psql $OSSPCKGS_DB_URL -f rollback.sql
 *
 * Flags:
 *   --dry-run   Print what would be inserted without touching the DB
 *
 * Usage:
 *   tsx src/maven/scripts/importMaintainersFromCsv.ts <input.csv> [--dry-run]
 */
import * as fs from 'fs'
import * as path from 'path'

import {
  insertPackageMaintainerLink,
  upsertMaintainer,
} from '@crowd/data-access-layer/src/osspckgs/maintainers'
import { findPackageIdsByPurl } from '@crowd/data-access-layer/src/osspckgs/packages'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { getPackagesDbConnection } from '../../db'

import { parseCsv } from './csv'

// ─── Role normalisation ───────────────────────────────────────────────────────

function normaliseRole(raw: string): 'author' | 'maintainer' | 'contributor' | null {
  if (raw === 'author') return 'author'
  if (raw === 'maintainer') return 'maintainer'
  if (raw === 'contributor') return 'contributor'
  return null
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const args = process.argv.slice(2)
  const dryRun = args.includes('--dry-run')
  const inputPath = args.find((a) => !a.startsWith('--'))

  if (!inputPath) {
    console.error(
      'Usage: tsx src/maven/scripts/importMaintainersFromCsv.ts <input.csv> [--dry-run]',
    )
    process.exit(1)
  }

  const rollbackPath = path.join(
    path.dirname(inputPath),
    path.basename(inputPath, '.csv') + '_rollback.sql',
  )

  console.log(`Input:    ${inputPath}`)
  console.log(`Rollback: ${rollbackPath}`)
  console.log(`Mode:     ${dryRun ? 'DRY RUN (no DB writes)' : 'LIVE'}`)
  console.log()

  const content = fs.readFileSync(inputPath, 'utf-8')
  const rows = parseCsv(content).filter((r) => r.package_purl && r.maintainer_github_login)

  console.log(`Rows with maintainer data: ${rows.length}`)

  // Group by purl
  const byPurl = new Map<string, typeof rows>()
  for (const r of rows) {
    const list = byPurl.get(r.package_purl) ?? []
    list.push(r)
    byPurl.set(r.package_purl, list)
  }
  console.log(`Unique packages to process: ${byPurl.size}`)

  if (dryRun) {
    // Just print a sample and exit
    let i = 0
    const entries = Array.from(byPurl.entries())
    for (const [purl, maintainers] of entries) {
      console.log(`  ${purl} → ${maintainers.length} maintainers`)
      if (++i >= 10) {
        console.log(`  ... (${byPurl.size - 10} more packages)`)
        break
      }
    }
    console.log('\nDry run complete — no changes made.')
    return
  }

  const conn = await getPackagesDbConnection()
  const qx = pgpQx(conn)

  // Batch-resolve all purls to package IDs in one query
  const purlList = [...byPurl.keys()]
  const purlToId = await findPackageIdsByPurl(qx, purlList)

  const missing = purlList.filter((p) => !purlToId.has(p))
  if (missing.length > 0) {
    console.warn(`\nWarning: ${missing.length} packages not found in DB (will be skipped):`)
    missing.slice(0, 10).forEach((p) => console.warn(`  ${p}`))
    if (missing.length > 10) console.warn(`  ... and ${missing.length - 10} more`)
  }

  const insertedPmIds: number[] = []
  let upsertedMaintainers = 0
  let insertedLinks = 0
  let skippedLinks = 0
  let skippedPackages = 0

  let processed = 0
  const total = byPurl.size

  for (const [purl, maintainerRows] of Array.from(byPurl.entries())) {
    const packageId = purlToId.get(purl)
    if (!packageId) {
      skippedPackages++
      continue
    }

    await conn.tx(async (t) => {
      const tqx = pgpQx(t)

      for (const r of maintainerRows) {
        // 1. Upsert the maintainer profile (github_login included via DAL)
        const { id: maintainerId } = await upsertMaintainer(tqx, {
          ecosystem: 'maven',
          username: r.maintainer_github_login,
          displayName: r.maintainer_display_name || null,
          url: r.maintainer_url || null,
          email: r.maintainer_email || null,
          githubLogin: r.maintainer_github_login || null,
        })
        upsertedMaintainers++

        // 2. Insert the package→maintainer link (non-destructive, via DAL)
        const role = normaliseRole(r.role)
        const insertedId = await insertPackageMaintainerLink(
          tqx,
          packageId,
          maintainerId,
          role,
          'manual_csv',
        )

        if (insertedId !== null) {
          insertedLinks++
          insertedPmIds.push(insertedId)
        } else {
          skippedLinks++
        }
      }
    })

    processed++
    if (processed % 50 === 0 || processed === total) {
      console.log(
        `[${processed}/${total}] maintainers=${upsertedMaintainers} links_inserted=${insertedLinks} links_skipped=${skippedLinks}`,
      )
    }
  }

  await (conn as unknown as { $pool: { end: () => Promise<void> } }).$pool.end()

  // Write rollback file
  const rollbackSql = [
    `-- Rollback for import: ${path.basename(inputPath)}`,
    `-- Generated: ${new Date().toISOString()}`,
    `-- Deletes only the package_maintainers rows inserted by this import.`,
    `-- Maintainer profiles in the maintainers table are left intact.`,
    ``,
    insertedPmIds.length > 0
      ? `DELETE FROM package_maintainers WHERE id IN (${insertedPmIds.join(', ')});`
      : `-- Nothing was inserted — no rows to roll back.`,
    ``,
    `-- Rows deleted: ${insertedPmIds.length}`,
  ].join('\n')

  fs.writeFileSync(rollbackPath, rollbackSql)

  console.log(`\n✓ Done.`)
  console.log(`  Packages processed:       ${processed}`)
  console.log(`  Packages skipped (not in DB): ${skippedPackages}`)
  console.log(`  Maintainer upserts:       ${upsertedMaintainers}`)
  console.log(`  package_maintainers inserted: ${insertedLinks}`)
  console.log(`  package_maintainers skipped (already existed): ${skippedLinks}`)
  console.log(`\n  Rollback file: ${rollbackPath}`)
  console.log(`  To revert: psql $OSSPCKGS_DB_URL -f ${rollbackPath}`)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
