import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { getDbConnection } from '@crowd/database'

import { createIngestJob, findPendingJobByKind, markJobStatus } from '../osspckgs/ingestJobs'
import type { QueryExecutor } from '../queryExecutor'
import { pgpQx } from '../queryExecutor'

import {
  createPackagistTransitiveRun,
  failPackagistTransitiveRun,
  findUnfinishedPackagistTransitiveRun,
  finishPackagistTransitiveRun,
  hasRecentDonePackagistTransitiveRun,
  markPackagistTransitiveRunMerging,
} from './packagistTransitiveRuns'
import {
  computePackagistTransitiveCounts,
  mergePackagistTransitiveCounts,
  snapshotPackagistDirectEdges,
} from './transitiveDependents'

// Integration test: hits the running packages-db, DESTRUCTIVELY — it drops and rebuilds
// the production-named staging.packagist_transitive_* tables, which would break a live
// transitive drain on a shared DB. Credentials alone are therefore not enough to run it:
// it also requires the explicit CROWD_PACKAGES_TESTS_DESTRUCTIVE=1 opt-in. Skipped
// automatically otherwise so unit-test runs in CI stay green.
const DESTRUCTIVE_OPT_IN = process.env.CROWD_PACKAGES_TESTS_DESTRUCTIVE === '1'
const HAVE_DB =
  !!process.env.CROWD_PACKAGES_DB_WRITE_HOST &&
  !!process.env.CROWD_PACKAGES_DB_PORT &&
  !!process.env.CROWD_PACKAGES_DB_USERNAME &&
  !!process.env.CROWD_PACKAGES_DB_DATABASE &&
  !!process.env.CROWD_PACKAGES_DB_PASSWORD

const FIXTURE_TAG = 'fable-transitive-dependents-fixture'
const VENDOR = 'fable-tdx'

// The packagist reverse transitive closure: collapse version-level direct edges to
// package-level pairs (snapshot), compute per-package transitive dependent counts
// (closure), merge them into packages.transitive_dependent_count (merge).
//
// Fixture graph (dep → subj means "dep depends on subj"):
//   collapse:  col-a v1 → col-x, col-a v2 → col-x (dedup), col-a v1 → col-dev (dev kind),
//              col-self → col-self (self), n1 → n2 (npm)
//   chain:     c1 → c2 → c3                        (c3: 1 transitive)
//   diamond:   d1 → {d2,d3}, {d2,d3} → d4          (d4: 1 transitive — d1 counted once)
//   cycle:     y1 → y2, y2 → y1, y3 → y1           (y2: 1 transitive — y3 through the cycle)
//   multipath: x1 → x3, x1 → x2, x2 → x3           (x3: 0 — x1 is direct, not double-counted)
//   leaf:      no edges at all                     (merge zero-fills 0)
describe.skipIf(!HAVE_DB || !DESTRUCTIVE_OPT_IN)(
  'packagist transitive dependents — real packages-db',
  () => {
    let qx: QueryExecutor

    const ids: Record<string, string> = {}
    const versionIds: Record<string, string> = {}
    const jobIds: number[] = []
    const runIds: number[] = []

    async function cleanupFixtures(): Promise<void> {
      await qx.result(
        `DELETE FROM package_dependencies WHERE package_id IN (
         SELECT id FROM packages WHERE ingestion_source = $(tag))`,
        { tag: FIXTURE_TAG },
      )
      await qx.result(
        `DELETE FROM versions WHERE package_id IN (
         SELECT id FROM packages WHERE ingestion_source = $(tag))`,
        { tag: FIXTURE_TAG },
      )
      await qx.result(`DELETE FROM packages WHERE ingestion_source = $(tag)`, { tag: FIXTURE_TAG })
      if (jobIds.length > 0) {
        await qx.result(`DELETE FROM osspckgs_ingest_jobs WHERE id = ANY($(jobIds)::int[])`, {
          jobIds,
        })
      }
      if (runIds.length > 0) {
        await qx.result(`DELETE FROM packagist_transitive_runs WHERE id = ANY($(runIds)::int[])`, {
          runIds,
        })
      }
      // The suites below destructively replace the REAL staging tables the live drain
      // reads — drop them so no fixture-only remnant can ever feed a later merge.
      await qx.result(`DROP TABLE IF EXISTS staging.packagist_transitive_edges`)
      await qx.result(`DROP TABLE IF EXISTS staging.packagist_transitive_counts`)
    }

    async function makePackage(name: string, ecosystem: 'packagist' | 'npm'): Promise<string> {
      const purl =
        ecosystem === 'packagist' ? `pkg:composer/${VENDOR}/${name}` : `pkg:npm/${VENDOR}-${name}`
      const row = await qx.selectOne(
        `INSERT INTO packages (purl, ecosystem, namespace, name, registry_url, status, ingestion_source)
       VALUES ($(purl), $(ecosystem), $(ns), $(name), 'https://example.test', 'active', $(tag))
       RETURNING id`,
        {
          purl,
          ecosystem,
          ns: ecosystem === 'packagist' ? VENDOR : null,
          name,
          tag: FIXTURE_TAG,
        },
      )
      ids[name] = String(row.id)
      return ids[name]
    }

    async function makeVersion(pkg: string, number: string): Promise<string> {
      const row = await qx.selectOne(
        `INSERT INTO versions (package_id, ecosystem, number, name, namespace)
       SELECT id, ecosystem, $(number), name, namespace FROM packages WHERE id = $(id)::bigint
       RETURNING id`,
        { id: ids[pkg], number },
      )
      versionIds[`${pkg}#${number}`] = String(row.id)
      return versionIds[`${pkg}#${number}`]
    }

    async function addEdge(
      pkg: string,
      number: string,
      toPkg: string,
      kind: 'direct' | 'dev',
    ): Promise<void> {
      await qx.result(
        `INSERT INTO package_dependencies
         (package_id, version_id, depends_on_id, version_constraint, dependency_kind, is_optional, created_at, updated_at)
       VALUES ($(pkgId)::bigint, $(verId)::bigint, $(depId)::bigint, '^1.0', $(kind), FALSE, NOW(), NOW())`,
        {
          pkgId: ids[pkg],
          verId: versionIds[`${pkg}#${number}`],
          depId: ids[toPkg],
          kind,
        },
      )
    }

    function minFixtureId(): string {
      const all = Object.values(ids).map((v) => BigInt(v))
      return String(all.reduce((a, b) => (a < b ? a : b)) - 1n)
    }

    async function drainMerge(limit: number): Promise<{ processed: number; changed: number }> {
      let cursor = minFixtureId()
      const totals = { processed: 0, changed: 0 }
      for (;;) {
        const r = await mergePackagistTransitiveCounts(qx, cursor, limit)
        totals.processed += r.processed
        totals.changed += r.changed
        if (r.processed < limit) return totals
        cursor = r.nextCursor
      }
    }

    async function transitiveOf(pkg: string): Promise<number | null> {
      const row = await qx.selectOne(
        `SELECT transitive_dependent_count FROM packages WHERE id = $(id)::bigint`,
        { id: ids[pkg] },
      )
      return row.transitive_dependent_count === null ? null : Number(row.transitive_dependent_count)
    }

    beforeAll(async () => {
      const conn = await getDbConnection({
        host: process.env.CROWD_PACKAGES_DB_WRITE_HOST ?? '',
        port: parseInt(process.env.CROWD_PACKAGES_DB_PORT ?? '0', 10),
        database: process.env.CROWD_PACKAGES_DB_DATABASE ?? '',
        user: process.env.CROWD_PACKAGES_DB_USERNAME ?? '',
        password: process.env.CROWD_PACKAGES_DB_PASSWORD ?? '',
      })
      qx = pgpQx(conn)
      await cleanupFixtures()

      const packagist = [
        'col-a',
        'col-x',
        'col-dev',
        'col-self',
        'c1',
        'c2',
        'c3',
        'd1',
        'd2',
        'd3',
        'd4',
        'y1',
        'y2',
        'y3',
        'x1',
        'x2',
        'x3',
        'leaf',
      ]
      for (const name of packagist) await makePackage(name, 'packagist')
      for (const name of ['n1', 'n2']) await makePackage(name, 'npm')

      for (const name of [...packagist, 'n1', 'n2']) {
        if (name === 'leaf') continue
        await makeVersion(name, '1.0.0')
      }
      await makeVersion('col-a', '2.0.0')

      await addEdge('col-a', '1.0.0', 'col-x', 'direct')
      await addEdge('col-a', '2.0.0', 'col-x', 'direct')
      await addEdge('col-a', '1.0.0', 'col-dev', 'dev')
      await addEdge('col-self', '1.0.0', 'col-self', 'direct')
      await addEdge('n1', '1.0.0', 'n2', 'direct')
      await addEdge('c1', '1.0.0', 'c2', 'direct')
      await addEdge('c2', '1.0.0', 'c3', 'direct')
      await addEdge('d1', '1.0.0', 'd2', 'direct')
      await addEdge('d1', '1.0.0', 'd3', 'direct')
      await addEdge('d2', '1.0.0', 'd4', 'direct')
      await addEdge('d3', '1.0.0', 'd4', 'direct')
      await addEdge('y1', '1.0.0', 'y2', 'direct')
      await addEdge('y2', '1.0.0', 'y1', 'direct')
      await addEdge('y3', '1.0.0', 'y1', 'direct')
      await addEdge('x1', '1.0.0', 'x3', 'direct')
      await addEdge('x1', '1.0.0', 'x2', 'direct')
      await addEdge('x2', '1.0.0', 'x3', 'direct')
    }, 60_000)

    afterAll(async () => {
      if (qx) await cleanupFixtures()
    })

    async function snapshotPairCount(dep: string, subj: string): Promise<number> {
      const row = await qx.selectOne(
        `SELECT COUNT(*) AS n FROM staging.packagist_transitive_edges
        WHERE dep = $(dep)::bigint AND subj = $(subj)::bigint`,
        { dep: ids[dep], subj: ids[subj] },
      )
      return Number(row.n)
    }

    // Runs against the REAL package_dependencies table (plus the fixture rows), so each
    // pass scans the full local dataset — hence the generous per-test timeouts.
    describe('snapshotPackagistDirectEdges — package-level collapse', () => {
      it('collapses version rows to distinct package edges, excluding dev/self/non-packagist', async () => {
        const edgeCount = await snapshotPackagistDirectEdges(qx)

        expect(edgeCount).toBeGreaterThan(0)
        const total = await qx.selectOne(
          `SELECT COUNT(*) AS n FROM staging.packagist_transitive_edges`,
        )
        expect(Number(total.n)).toBe(edgeCount)

        // two versions of col-a require col-x → exactly one package-level edge
        expect(await snapshotPairCount('col-a', 'col-x')).toBe(1)
        // require-dev is not installed transitively by Composer → excluded
        expect(await snapshotPairCount('col-a', 'col-dev')).toBe(0)
        // self-edges excluded
        expect(await snapshotPairCount('col-self', 'col-self')).toBe(0)
        // non-packagist ecosystems excluded
        const npm = await qx.selectOne(
          `SELECT COUNT(*) AS n FROM staging.packagist_transitive_edges WHERE dep = $(id)::bigint`,
          { id: ids.n1 },
        )
        expect(Number(npm.n)).toBe(0)
        // ordinary direct edge present
        expect(await snapshotPairCount('c1', 'c2')).toBe(1)
      }, 120_000)

      it('is re-runnable — a second snapshot fully replaces the first', async () => {
        const edgeCount = await snapshotPackagistDirectEdges(qx)
        const total = await qx.selectOne(
          `SELECT COUNT(*) AS n FROM staging.packagist_transitive_edges`,
        )
        expect(Number(total.n)).toBe(edgeCount)
        expect(await snapshotPairCount('col-a', 'col-x')).toBe(1)
      }, 120_000)
    })

    describe('closure + merge — fixture-only edge set', () => {
      let packagesWithDependents: number

      // Rebuild the snapshot table with ONLY the fixture's package-level pairs so the
      // closure output is fully deterministic (the snapshot suite above owns testing the
      // collapse itself; this suite owns the closure/merge arithmetic).
      beforeAll(async () => {
        await qx.result(`DROP TABLE IF EXISTS staging.packagist_transitive_edges`)
        await qx.result(
          `CREATE UNLOGGED TABLE staging.packagist_transitive_edges (dep bigint NOT NULL, subj bigint NOT NULL)`,
        )
        const pairs: Array<[string, string]> = [
          ['col-a', 'col-x'],
          ['c1', 'c2'],
          ['c2', 'c3'],
          ['d1', 'd2'],
          ['d1', 'd3'],
          ['d2', 'd4'],
          ['d3', 'd4'],
          ['y1', 'y2'],
          ['y2', 'y1'],
          ['y3', 'y1'],
          ['x1', 'x3'],
          ['x1', 'x2'],
          ['x2', 'x3'],
        ]
        for (const [dep, subj] of pairs) {
          await qx.result(
            `INSERT INTO staging.packagist_transitive_edges (dep, subj)
           VALUES ($(dep)::bigint, $(subj)::bigint)`,
            { dep: ids[dep], subj: ids[subj] },
          )
        }
        await qx.result(`CREATE INDEX ON staging.packagist_transitive_edges (subj, dep)`)

        packagesWithDependents = await computePackagistTransitiveCounts(qx)
      }, 30_000)

      async function countsRow(pkg: string): Promise<number | null> {
        const row = await qx.selectOneOrNone(
          `SELECT transitive_dependent_count FROM staging.packagist_transitive_counts
          WHERE package_id = $(id)::bigint`,
          { id: ids[pkg] },
        )
        return row === null ? null : Number(row.transitive_dependent_count)
      }

      it('produces one counts row per package with at least one dependent', async () => {
        // subjects with ≥1 dependent: col-x, c2, c3, d2, d3, d4, y1, y2, x2, x3
        expect(packagesWithDependents).toBe(10)
        const total = await qx.selectOne(
          `SELECT COUNT(*) AS n FROM staging.packagist_transitive_counts`,
        )
        expect(Number(total.n)).toBe(10)
        // packages nobody depends on have no row (merge zero-fills them)
        expect(await countsRow('c1')).toBeNull()
        expect(await countsRow('y3')).toBeNull()
      })

      it('counts dependents at depth ≥ 2, excluding direct ones', async () => {
        expect(await countsRow('c3')).toBe(1) // c1 via c2
        expect(await countsRow('c2')).toBe(0) // only the direct c1
        expect(await countsRow('col-x')).toBe(0)
      })

      it('dedups diamond paths — one ancestor counted once', async () => {
        expect(await countsRow('d4')).toBe(1) // d1, via both d2 and d3
        expect(await countsRow('d2')).toBe(0)
        expect(await countsRow('d3')).toBe(0)
      })

      it('terminates on cycles, never counts a package as its own dependent', async () => {
        expect(await countsRow('y1')).toBe(0) // y2 and y3 are both direct
        expect(await countsRow('y2')).toBe(1) // y3 reaches y2 through the cycle
      })

      it('counts a dependent that is both direct and transitive as direct only', async () => {
        expect(await countsRow('x3')).toBe(0) // x1 is direct even though x1→x2→x3 also exists
      })

      it('merges counts into packages, zero-fills edge-less packagist rows, skips other ecosystems', async () => {
        await drainMerge(200)

        expect(await transitiveOf('c3')).toBe(1)
        expect(await transitiveOf('c2')).toBe(0)
        expect(await transitiveOf('d4')).toBe(1)
        expect(await transitiveOf('y2')).toBe(1)
        expect(await transitiveOf('x3')).toBe(0)
        // packages with no dependents (or no edges at all) get 0, not NULL
        expect(await transitiveOf('c1')).toBe(0)
        expect(await transitiveOf('y3')).toBe(0)
        expect(await transitiveOf('leaf')).toBe(0)
        expect(await transitiveOf('col-dev')).toBe(0)
        expect(await transitiveOf('col-self')).toBe(0)
        // other ecosystems are never touched
        expect(await transitiveOf('n2')).toBeNull()
      })

      it('is churn-free on re-run — unchanged rows keep their last_synced_at', async () => {
        const before = await qx.selectOne(
          `SELECT last_synced_at FROM packages WHERE id = $(id)::bigint`,
          { id: ids.c3 },
        )

        const totals = await drainMerge(200)
        expect(totals.changed).toBe(0)

        const after = await qx.selectOne(
          `SELECT last_synced_at FROM packages WHERE id = $(id)::bigint`,
          { id: ids.c3 },
        )
        expect(after.last_synced_at).toEqual(before.last_synced_at)
      })

      it('paginates by keyset — respects the limit and resumes from the cursor', async () => {
        const first = await mergePackagistTransitiveCounts(qx, minFixtureId(), 3)
        expect(first.processed).toBe(3)
        expect(first.nextCursor).not.toBe('')

        const second = await mergePackagistTransitiveCounts(qx, first.nextCursor, 3)
        expect(second.processed).toBe(3)
        expect(BigInt(second.nextCursor)).toBeGreaterThan(BigInt(first.nextCursor))
      })

      // Last on purpose: it empties the counts table the earlier cases depend on. The
      // UNLOGGED staging table is truncated by crash recovery, and the zero-fill merge
      // would otherwise read that as "every package is a leaf" and wipe real counts.
      it('refuses to merge when the counts staging table is empty', async () => {
        await qx.result(`TRUNCATE staging.packagist_transitive_counts`)

        await expect(mergePackagistTransitiveCounts(qx, minFixtureId(), 3)).rejects.toThrow(
          /empty/i,
        )
      })
    })

    describe('findPendingJobByKind', () => {
      it('returns the newest pending job of the kind; a finished job is no longer returned', async () => {
        const jobA = await createIngestJob(qx, 'ranking', 'ranking', null)
        jobIds.push(jobA)
        const jobB = await createIngestJob(qx, 'ranking', 'ranking', null)
        jobIds.push(jobB)

        // createIngestJob returns the raw bigserial id (a string at runtime);
        // findPendingJobByKind normalizes to number — compare accordingly.
        const found = await findPendingJobByKind(qx, 'ranking')
        expect(found).toBe(Number(jobB))

        await markJobStatus(qx, jobB, 'done', { finishedAt: new Date() })
        const next = await findPendingJobByKind(qx, 'ranking')
        expect(next).not.toBe(Number(jobB))
      })
    })

    describe('packagist_transitive_runs ledger', () => {
      it('walks the run lifecycle: pending (reusable) → merging with graph sizes → done with totals', async () => {
        const runA = await createPackagistTransitiveRun(qx)
        runIds.push(runA)
        const runB = await createPackagistTransitiveRun(qx)
        runIds.push(runB)

        // newest unfinished wins — a Temporal retry reuses it instead of minting a third
        expect(await findUnfinishedPackagistTransitiveRun(qx)).toBe(runB)

        await markPackagistTransitiveRunMerging(qx, runB, {
          edgeCount: 918346,
          packagesWithDependents: 85600,
        })
        // still adoptable while merging — a retry whose completion was lost after the
        // commit must find the row it already marked, not mint a duplicate
        expect(await findUnfinishedPackagistTransitiveRun(qx)).toBe(runB)

        await finishPackagistTransitiveRun(qx, runB, { processed: 454455, changed: 86000 })
        // finished runs are no longer adoptable — the older pending row surfaces again
        expect(await findUnfinishedPackagistTransitiveRun(qx)).toBe(runA)

        const row = await qx.selectOne(
          `SELECT status, edge_count, packages_with_dependents, processed_rows, changed_rows, finished_at
           FROM packagist_transitive_runs WHERE id = $(id)`,
          { id: runB },
        )
        expect(row.status).toBe('done')
        // window arithmetic: the fresh 'done' row is inside a 7-day window; a zero-day
        // window can match nothing (nothing finishes in the future)
        expect(await hasRecentDonePackagistTransitiveRun(qx, 7)).toBe(true)
        expect(await hasRecentDonePackagistTransitiveRun(qx, 0)).toBe(false)
        expect(Number(row.edge_count)).toBe(918346)
        expect(Number(row.packages_with_dependents)).toBe(85600)
        expect(Number(row.processed_rows)).toBe(454455)
        expect(Number(row.changed_rows)).toBe(86000)
        expect(row.finished_at).not.toBeNull()
      })

      it('records terminal failure with the error message', async () => {
        const run = await createPackagistTransitiveRun(qx)
        runIds.push(run)

        await failPackagistTransitiveRun(qx, run, 'closure exploded')

        const row = await qx.selectOne(
          `SELECT status, error_message, finished_at FROM packagist_transitive_runs WHERE id = $(id)`,
          { id: run },
        )
        expect(row.status).toBe('failed')
        expect(row.error_message).toBe('closure exploded')
        expect(row.finished_at).not.toBeNull()
      })
    })
  },
)
