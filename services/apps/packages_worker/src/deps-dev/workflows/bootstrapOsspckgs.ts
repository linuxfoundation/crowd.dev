import {
  ApplicationFailure,
  ChildWorkflowFailure,
  executeChild,
  proxyActivities,
  workflowInfo,
} from '@temporalio/workflow'

import { rankPackagesWorkflow } from '../../criticality/workflow'
import { ingestScorecard } from '../../scorecard/workflows'
import type * as depsDevActivities from '../activities'

import { ingestAdvisories } from './ingestAdvisories'
import { ingestDependencies } from './ingestDependencies'
import { ingestDependentCounts } from './ingestDependentCounts'
import { ingestPackages } from './ingestPackages'
import { ingestRepos } from './ingestRepos'
import { ingestVersions } from './ingestVersions'

const { getLastSnapshot, probePartitionExists, resolveSnapshotDate } = proxyActivities<
  typeof depsDevActivities
>({
  startToCloseTimeout: '5 minutes',
  retry: { maximumAttempts: 3 },
})

type JobKind =
  | 'packages'
  | 'repos'
  | 'versions'
  | 'package_repos'
  | 'package_dependencies'
  | 'advisories'
  | 'advisory_packages'
  | 'dependent_counts'
  | 'dependent_counts_go'
  | 'dependent_counts_nuget'

// deps.dev retains weekly snapshots for ~3 years; 1095 days (3 years) gives comfortable headroom.
// advisories/advisory_packages use AdvisoriesLatest (no partition history) → effectively unlimited.
// dependent_counts_go/_nuget read *RequirementsLatest (no partition history) → effectively unlimited.
const RETENTION_DAYS_BY_KIND: Record<JobKind, number> = {
  packages: 1095,
  repos: 1095,
  versions: 1095,
  package_repos: 1095,
  package_dependencies: 1095,
  advisories: 999_999,
  advisory_packages: 999_999,
  dependent_counts: 1095,
  dependent_counts_go: 999_999,
  dependent_counts_nuget: 999_999,
}

// Kinds whose incremental diff is driven by a BQ partition snapshot date.
// repos/package_repos use cursor-based ingestion; advisories/advisory_packages use Latest views.
const PARTITIONED_KINDS: JobKind[] = ['packages', 'versions', 'package_dependencies']

// Kinds that query PackageVersionToProject/Dependents — always resolve snapshot date because those
// tables are on a weekly cadence that may differ from PackageVersions.
const SNAPSHOT_RESOLVED_KINDS: JobKind[] = ['repos', 'dependent_counts']

export async function bootstrapOsspckgs(opts: {
  mode: 'full' | 'incremental'
  ecosystems?: string[]
  kinds?: string[]
  reuseExports?: boolean
  depsTableOption?: 'A' | 'B'
  exportName?: string
  snapshotDate?: string // YYYY-MM-DD — override BQ snapshot resolution for all partition-filtered kinds
  fillConstraints?: boolean // re-export full deps BQ data, upsert version_constraint where NULL
  resumeJobId?: number // resume a partially-merged package_dependencies job by id (skips its BQ export)
}): Promise<void> {
  // B3: deterministic timestamps — workflowInfo().startTime is replay-stable; new Date() is not.
  const start = workflowInfo().startTime
  const runId = start.toISOString().replace(/[:.]/g, '-')
  const today = start.toISOString().slice(0, 10)

  // Resume mode reuses a prior job's export, so there is no fresh BQ export to validate. Skip the
  // incremental watermark/partition checks below — the resumed partition may not match `today`.
  const resume = opts.resumeJobId != null

  // Recovery: each child workflow updates osspckgs_ingest_jobs independently.
  // If a child fails mid-bootstrap, re-run with the SAME mode.
  // Already-done kinds skip naturally (today→today diff = 0 rows).
  // Failed kinds resume from their last successful snapshot.
  //
  // First-ever bootstrap (no prior snapshots): must run mode='full'.
  // The 'full' scan against *Latest views is naturally idempotent — re-running
  // after a partial failure re-processes already-loaded rows via ON CONFLICT DO NOTHING,
  // which is wasteful but safe.

  const activeKinds = opts.kinds ? new Set(opts.kinds) : null
  const runs = (kind: string) => !activeKinds || activeKinds.has(kind)

  // Resume reuses a prior package_dependencies export and skips watermark/partition validation.
  // Hard-enforce it targets ONLY package_dependencies so a stray resumeJobId can't silently run other
  // kinds without their safety checks. The CLI validates this too; this is the fail-fast backstop.
  if (
    resume &&
    !(activeKinds && activeKinds.size === 1 && activeKinds.has('package_dependencies'))
  ) {
    throw new ApplicationFailure(
      'resumeJobId is only valid with kinds=[package_dependencies] — refusing to skip validation for other kinds',
    )
  }

  const jobKinds: JobKind[] = (
    ['packages', 'versions', 'package_dependencies', 'advisories', 'advisory_packages'] as JobKind[]
  ).filter((k) => runs(k))
  const watermarks = new Map<JobKind, string | null>()

  // B6: resolve the actual available snapshot date from BQ (deps.dev publishes weekly, not daily).
  // Partitioned BQ kinds need resolved dates; cursor kinds (repos, dependent_counts) query different
  // tables (PackageVersionToProject, Dependents) that may be on a different cadence than PackageVersions.
  const resolvedSnapshots = new Map<JobKind, string>()

  // Partitioned kinds: always resolve so snapshot_at stores a real BQ partition date.
  // Full mode doesn't use the date in its BQ query (*Latest views, no partition filter)
  // but the resolved date becomes the watermark for the next incremental run.
  for (const kind of PARTITIONED_KINDS.filter((k) => runs(k))) {
    const snapshotDate =
      opts.snapshotDate ?? (await resolveSnapshotDate({ jobKind: kind, today })).snapshotDate
    resolvedSnapshots.set(kind, snapshotDate)
  }

  // Always resolve snapshot date for kinds that filter by partition date.
  // repos/package_repos share one ingestRepos call — resolve repos snapshot when either runs.
  for (const kind of SNAPSHOT_RESOLVED_KINDS) {
    const shouldResolve = kind === 'repos' ? runs('repos') || runs('package_repos') : runs(kind)
    if (!shouldResolve) continue
    const snapshotDate =
      opts.snapshotDate ?? (await resolveSnapshotDate({ jobKind: kind, today })).snapshotDate
    resolvedSnapshots.set(kind, snapshotDate)
  }

  // Validate all watermarks up-front before touching BQ (fail fast, not mid-run)
  for (const jobKind of jobKinds) {
    if (opts.mode === 'incremental' && !resume) {
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
  if (runs('packages')) {
    await executeChild(ingestPackages, {
      args: [
        {
          runId,
          syncMode: opts.mode,
          today: snap('packages'),
          watermark: wm('packages'),
          ecosystems: opts.ecosystems,
          reuseExports: opts.reuseExports,
          exportName: opts.exportName,
        },
      ],
    })
  }
  if (runs('dependent_counts')) {
    try {
      await executeChild(ingestDependentCounts, {
        args: [
          {
            runId,
            snapshotDate: snap('dependent_counts'),
            reuseExports: opts.reuseExports,
            exportName: opts.exportName,
          },
        ],
      })
    } catch (err) {
      // Only soft-fail on the row-count guard. Child workflow failures are wrapped in
      // ChildWorkflowFailure — unwrap to inspect the cause.
      // All other errors (BQ timeout, DB failure, etc.) propagate normally.
      const cause = err instanceof ChildWorkflowFailure ? err.cause : err
      if (!(cause instanceof ApplicationFailure) || cause.type !== 'DEPENDENT_COUNTS_GUARD') {
        throw err
      }
    }
  }
  // GO/NUGET reverse-dependent counts: separate kinds, manifest-sourced (GoRequirementsLatest /
  // NuGetRequirementsLatest), computed via the exact reverse transitive closure script. The manifests
  // are *Latest views (no resolution needed); `today` is the snapshot_at stamp AND the anchor for the
  // dependent_repos partition window (latest PackageVersionToProject snapshot within 60 days). Each
  // guards against its own history and merges a disjoint purl space, so an edge-snapshot corruption
  // that aborts `dependent_counts` never blocks these.
  for (const variant of ['go', 'nuget'] as const) {
    const kind = `dependent_counts_${variant}` as const
    if (!runs(kind)) continue
    try {
      await executeChild(ingestDependentCounts, {
        args: [
          {
            runId,
            // Honor --snapshot-date like the partition kinds do (snap()). The closure reads *Latest
            // manifests regardless, but anchors the dependent_repos 60-day window on this date, so a
            // recovery run with an override must use it for a consistent window.
            snapshotDate: opts.snapshotDate ?? today,
            variant,
            reuseExports: opts.reuseExports,
            exportName: opts.exportName,
          },
        ],
        workflowId: `${runId}-${kind}`,
      })
    } catch (err) {
      // Soft-fail only on the row-count guard, mirroring the edge dependent_counts handling above.
      const cause = err instanceof ChildWorkflowFailure ? err.cause : err
      if (!(cause instanceof ApplicationFailure) || cause.type !== 'DEPENDENT_COUNTS_GUARD') {
        throw err
      }
    }
  }
  if (runs('repos') || runs('package_repos')) {
    await executeChild(ingestRepos, {
      args: [
        {
          runId,
          snapshotDate: snap('repos'),
          ecosystems: opts.ecosystems,
          reuseExports: opts.reuseExports,
          exportName: opts.exportName,
        },
      ],
    })
  }
  if (runs('versions')) {
    await executeChild(ingestVersions, {
      args: [
        {
          runId,
          syncMode: opts.mode,
          today: snap('versions'),
          watermark: wm('versions'),
          ecosystems: opts.ecosystems,
          reuseExports: opts.reuseExports,
          exportName: opts.exportName,
        },
      ],
    })
  }
  if (runs('package_dependencies')) {
    try {
      await executeChild(ingestDependencies, {
        args: [
          {
            runId,
            syncMode: opts.mode,
            today: snap('package_dependencies'),
            watermark: wm('package_dependencies'),
            ecosystems: opts.ecosystems,
            reuseExports: opts.reuseExports,
            depsTableOption: opts.depsTableOption,
            exportName: opts.exportName,
            fillConstraints: opts.fillConstraints,
            resumeJobId: opts.resumeJobId,
          },
        ],
      })
    } catch (err) {
      // Only soft-fail on the edge-snapshot quality guard (corrupt deps.dev resolved-graph
      // snapshot). Skipping leaves existing package_dependencies untouched and lets the rest
      // of the bootstrap proceed; the next healthy snapshot ingests naturally. Mirror the
      // dependent_counts guard: unwrap the ChildWorkflowFailure to inspect the cause; all
      // other errors propagate.
      const cause = err instanceof ChildWorkflowFailure ? err.cause : err
      if (!(cause instanceof ApplicationFailure) || cause.type !== 'EDGE_SNAPSHOT_GUARD') {
        throw err
      }
    }
  }
  if (runs('advisories') || runs('advisory_packages')) {
    await executeChild(ingestAdvisories, {
      args: [
        {
          runId,
          syncMode: opts.mode,
          today,
          watermark: wm('advisories'),
          ecosystems: opts.ecosystems,
          reuseExports: opts.reuseExports,
          exportName: opts.exportName,
        },
      ],
    })
  }
  if (runs('scorecard')) {
    await executeChild(ingestScorecard, {
      args: [{ runId, reuseExports: opts.reuseExports, exportName: opts.exportName }],
    })
  }
  if (runs('ranking')) {
    await executeChild(rankPackagesWorkflow, { args: [] })
  }
}
