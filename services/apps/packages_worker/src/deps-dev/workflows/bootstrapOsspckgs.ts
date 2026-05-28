import { ApplicationFailure, executeChild, proxyActivities, workflowInfo } from '@temporalio/workflow'

import type * as depsDevActivities from '../activities'

import { ingestAdvisories } from './ingestAdvisories'
import { ingestDependencies } from './ingestDependencies'
import { ingestPackages } from './ingestPackages'
import { ingestRepos } from './ingestRepos'
import { ingestVersions } from './ingestVersions'

const { getLastSnapshot, probePartitionExists, resolveSnapshotDate } =
  proxyActivities<typeof depsDevActivities>({
    startToCloseTimeout: '5 minutes',
    retry: { maximumAttempts: 3 },
  })

const { rankPackagesUniverse, updateDependentCounts } = proxyActivities<typeof depsDevActivities>({
  startToCloseTimeout: '1 hour',
  retry: { maximumAttempts: 2 },
})

type JobKind =
  | 'packages'
  | 'repos'
  | 'versions'
  | 'package_repos'
  | 'package_dependencies'
  | 'advisories'
  | 'advisory_packages'

// deps.dev retains weekly snapshots for ~3 years; 1095 days (3 years) gives comfortable headroom.
// advisories/advisory_packages use AdvisoriesLatest (no partition history) → effectively unlimited.
const RETENTION_DAYS_BY_KIND: Record<JobKind, number> = {
  packages: 1095,
  repos: 1095,
  versions: 1095,
  package_repos: 1095,
  package_dependencies: 1095,
  advisories: 999_999,
  advisory_packages: 999_999,
}

// Kinds whose incremental diff is driven by a BQ partition snapshot date.
// repos/package_repos use cursor-based ingestion; advisories/advisory_packages use Latest views.
const PARTITIONED_KINDS: JobKind[] = ['packages', 'versions', 'package_dependencies']

export async function bootstrapOsspckgs(opts: { mode: 'full' | 'incremental' }): Promise<void> {
  // B3: deterministic timestamps — workflowInfo().startTime is replay-stable; new Date() is not.
  const start = workflowInfo().startTime
  const runId = start.toISOString().replace(/[:.]/g, '-')
  const today = start.toISOString().slice(0, 10)

  // Recovery: each child workflow updates osspckgs_ingest_jobs independently.
  // If a child fails mid-bootstrap, re-run with the SAME mode.
  // Already-done kinds skip naturally (today→today diff = 0 rows).
  // Failed kinds resume from their last successful snapshot.
  //
  // First-ever bootstrap (no prior snapshots): must run mode='full'.
  // The 'full' scan against *Latest views is naturally idempotent — re-running
  // after a partial failure re-processes already-loaded rows via ON CONFLICT,
  // which is wasteful but safe.

  const jobKinds: JobKind[] = [
    'packages', 'versions', 'package_dependencies', 'advisories', 'advisory_packages',
  ]
  const watermarks = new Map<JobKind, string | null>()

  // B6: resolve the actual available snapshot date from BQ (deps.dev publishes weekly, not daily).
  // All partitioned kinds share the same PackageVersions/DependencyGraphEdges cadence so one
  // resolution covers all — but each kind may in theory differ, so resolve per-kind.
  const resolvedSnapshots = new Map<JobKind, string>()
  if (opts.mode === 'incremental') {
    for (const kind of PARTITIONED_KINDS) {
      const { snapshotDate } = await resolveSnapshotDate({ jobKind: kind, today })
      resolvedSnapshots.set(kind, snapshotDate)
    }
  }

  // Validate all watermarks up-front before touching BQ (fail fast, not mid-run)
  for (const jobKind of jobKinds) {
    if (opts.mode === 'incremental') {
      const { snapshotAt } = await getLastSnapshot({ jobKind })
      if (!snapshotAt) {
        throw new ApplicationFailure(`No watermark for ${jobKind} — run full bootstrap first`)
      }
      const resolvedToday = resolvedSnapshots.get(jobKind as JobKind) ?? today
      const gapDays = Math.floor(
        (new Date(resolvedToday).getTime() - new Date(snapshotAt).getTime()) / 86_400_000,
      )
      const maxDays = RETENTION_DAYS_BY_KIND[jobKind]
      if (gapDays > maxDays) {
        throw new ApplicationFailure(
          `${jobKind} watermark ${snapshotAt} is ${gapDays}d old (retention ${maxDays}d) — trigger full re-bootstrap`,
        )
      }
      if (PARTITIONED_KINDS.includes(jobKind as JobKind)) {
        await probePartitionExists({ jobKind, snapshotAt: resolvedToday })
      }
      watermarks.set(jobKind, snapshotAt)
    } else {
      watermarks.set(jobKind, null)
    }
  }

  const wm = (kind: JobKind): string | null => watermarks.get(kind) ?? null
  const snap = (kind: JobKind): string => resolvedSnapshots.get(kind) ?? today

  // FK order: packages first (no inbound FKs), then repos, then package_repos
  // (FK → packages + repos), then versions (FK → packages), then deps (FK → versions),
  // then advisories last.
  // M3: executeChild for retry isolation and history compaction per kind.
  await executeChild(ingestPackages, {
    args: [{ runId, syncMode: opts.mode, today: snap('packages'), watermark: wm('packages') }],
  })
  await updateDependentCounts({ runId })

  await executeChild(ingestRepos, { args: [{ runId }] })

  await executeChild(ingestVersions, {
    args: [{ runId, syncMode: opts.mode, today: snap('versions'), watermark: wm('versions') }],
  })
  await executeChild(ingestDependencies, {
    args: [{ runId, syncMode: opts.mode, today: snap('package_dependencies'), watermark: wm('package_dependencies') }],
  })
  await rankPackagesUniverse()
  await executeChild(ingestAdvisories, {
    args: [{ runId, syncMode: opts.mode, today, watermark: wm('advisories') }],
  })
}
