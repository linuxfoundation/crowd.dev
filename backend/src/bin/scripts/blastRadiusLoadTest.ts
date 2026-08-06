// Local load-test harness for the blast-radius Temporal pipeline. Not meant to
// ship — this is a dev-only tool, kept under bin/scripts like other one-offs.
//
// Starts N real analyzeBlastRadius workflows directly via the Temporal client
// (bypassing the public HTTP/Zod/Auth0 layer, same as prod's own submit path
// underneath), optionally capping the worker container's memory and stopping
// each workflow right after a chosen stage via `stopAfterStage` — so this can
// safely profile stage 2 (dependents, no LLM) without ever reaching the paid
// stage 3 (reachability, Sonnet) unless explicitly asked to.
//
// Usage:
//   cd backend && npx tsx src/bin/scripts/blastRadiusLoadTest.ts \
//     --jobs=8 --scanConcurrency=8 --memCap=2g --stopAfter=dependents \
//     --advisories=src/bin/scripts/blastRadiusLoadTestAdvisories.json
//
// Flags (all optional):
//   --jobs=N            number of concurrent analyses to start (default 8)
//   --scanConcurrency=N sets BLAST_RADIUS_SCAN_CONCURRENCY on the worker container
//                        for the duration of this run, then clears it (default: unset)
//   --memCap=2g          docker memory cap applied to the worker container for the
//                        duration of this run, then reset to unlimited (default: none)
//   --stopAfter=STAGE    'intel' | 'dependents' | 'reachability' — workflow stops
//                        right after this stage succeeds (default: 'dependents')
//   --container=NAME     worker container name (default crowd_blast-radius-worker-dev_1)
//   --advisories=FILE    path to a JSON array of {advisoryId, package, ecosystem} —
//                        jobs round-robin across these instead of all hitting the
//                        same package (default: a single lodash advisory, repeated)
import { execSync } from 'child_process'
import * as fs from 'fs'

import { generateUUIDv4 } from '@crowd/common'
import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { TemporalWorkflowId } from '@crowd/types'

import { getPackagesQx } from '@/db/packagesDb'
import { getPackagesTemporalClient } from '@/db/packagesTemporal'

function flag(name: string, fallback?: string): string | undefined {
  const arg = process.argv.find((a) => a.startsWith(`--${name}=`))
  return arg ? arg.slice(name.length + 3) : fallback
}

const JOBS = Number(flag('jobs', '8'))
const SCAN_CONCURRENCY = flag('scanConcurrency')
const MEM_CAP = flag('memCap')
const STOP_AFTER = flag('stopAfter', 'dependents') as 'intel' | 'dependents' | 'reachability'
const CONTAINER = flag('container', 'crowd_blast-radius-worker-dev_1')
const ADVISORIES_FILE = flag('advisories')

const POLL_INTERVAL_MS = 5_000
const TIMEOUT_MS = 60 * 60 * 1000

interface AdvisoryTarget {
  advisoryId: string
  package: string
  ecosystem: string
}

const DEFAULT_ADVISORIES: AdvisoryTarget[] = [
  { advisoryId: 'GHSA-jf85-cpcp-j695', package: 'lodash', ecosystem: 'npm' }, // real OSV.dev entry, validated
]

function loadAdvisories(): AdvisoryTarget[] {
  if (!ADVISORIES_FILE) return DEFAULT_ADVISORIES
  const parsed = JSON.parse(fs.readFileSync(ADVISORIES_FILE, 'utf-8'))
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error(`--advisories file must contain a non-empty JSON array: ${ADVISORIES_FILE}`)
  }
  return parsed
}

function sh(cmd: string): string {
  return execSync(cmd, { encoding: 'utf-8' }).trim()
}

function applyRunConfig() {
  if (MEM_CAP) {
    console.log(`[loadtest] capping ${CONTAINER} memory at ${MEM_CAP}`)
    sh(`docker update --memory=${MEM_CAP} --memory-swap=${MEM_CAP} ${CONTAINER}`)
  }
  if (SCAN_CONCURRENCY) {
    // Env vars can't be changed on an already-running container (unlike the memory
    // cgroup cap above, which docker update can patch live) — they're baked in at
    // container start. Verify the worker already has the value this run wants
    // instead of silently testing against whatever it happened to start with.
    const actual = sh(`docker exec ${CONTAINER} sh -c 'echo $BLAST_RADIUS_SCAN_CONCURRENCY'`)
    if (actual !== SCAN_CONCURRENCY) {
      throw new Error(
        `--scanConcurrency=${SCAN_CONCURRENCY} requested but ${CONTAINER} was started with ` +
          `BLAST_RADIUS_SCAN_CONCURRENCY=${actual || '(unset)'}. Restart it first: ` +
          `BLAST_RADIUS_SCAN_CONCURRENCY=${SCAN_CONCURRENCY} ./scripts/cli service blast-radius-worker restart`,
      )
    }
    console.log(`[loadtest] confirmed BLAST_RADIUS_SCAN_CONCURRENCY=${SCAN_CONCURRENCY} on worker`)
  }
}

function resetRunConfig() {
  console.log('[loadtest] resetting container memory cap to unlimited')
  try {
    sh(`docker update --memory=0 --memory-swap=0 ${CONTAINER}`)
  } catch {
    // some docker versions reject 0; fall back to a generous cap instead of leaving 2g stuck
    try {
      sh(`docker update --memory=8g --memory-swap=8g ${CONTAINER}`)
    } catch {
      console.warn('[loadtest] could not reset memory cap automatically — check manually')
    }
  }
}

function sampleContainerStats(): { memUsage: string; cpuPerc: string } | null {
  try {
    const raw = sh(`docker stats ${CONTAINER} --no-stream --format "{{.MemUsage}}|{{.CPUPerc}}"`)
    const [memUsage, cpuPerc] = raw.split('|')
    return { memUsage, cpuPerc }
  } catch {
    return null
  }
}

function percentile(values: number[], p: number): number | null {
  if (values.length === 0) return null
  const sorted = [...values].sort((a, b) => a - b)
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))
  return sorted[idx]
}

function stats(values: number[]) {
  if (values.length === 0) return null
  return {
    count: values.length,
    min: Math.min(...values),
    avg: Math.round(values.reduce((a, b) => a + b, 0) / values.length),
    p95: percentile(values, 95),
    max: Math.max(...values),
  }
}

async function main() {
  const advisories = loadAdvisories()
  console.log(
    `[loadtest] jobs=${JOBS} scanConcurrency=${SCAN_CONCURRENCY ?? '(default 32)'} ` +
      `memCap=${MEM_CAP ?? '(none)'} stopAfter=${STOP_AFTER} container=${CONTAINER} ` +
      `advisories=${advisories.length}${ADVISORIES_FILE ? ` (${ADVISORIES_FILE})` : ' (default)'}`,
  )

  applyRunConfig()

  const qx = await getPackagesQx()
  const packagesTemporal = await getPackagesTemporalClient()

  const analysisIds: string[] = []
  const submittedAt = Date.now()

  try {
    await Promise.all(
      Array.from({ length: JOBS }, async (_, i) => {
        const target = advisories[i % advisories.length]
        const analysisId = generateUUIDv4()
        const analysisInput = {
          id: analysisId,
          advisoryOsvId: target.advisoryId,
          packageName: target.package,
          ecosystem: target.ecosystem,
          force: false,
        }
        await blastRadiusDal.createAnalysis(qx, analysisInput)
        await packagesTemporal.workflow.start('analyzeBlastRadius', {
          taskQueue: 'blast-radius-worker',
          workflowId: `${TemporalWorkflowId.BLAST_RADIUS_ANALYSIS}/${analysisId}`,
          retry: { maximumAttempts: 1 },
          args: [
            {
              analysisId,
              advisoryId: target.advisoryId,
              package: target.package,
              ecosystem: target.ecosystem,
              force: false,
              stopAfterStage: STOP_AFTER,
            },
          ],
        })
        analysisIds.push(analysisId)
      }),
    )

    console.log(
      `[loadtest] submitted ${analysisIds.length} analyses in ${Date.now() - submittedAt}ms`,
    )
    console.log(`[loadtest] analysisIds: ${analysisIds.join(', ')}`)

    const deadline = Date.now() + TIMEOUT_MS
    let allDone = false
    const memSamples: string[] = []

    while (Date.now() < deadline) {
      // With stopAfterStage, the analysis row itself stays 'running' forever (the
      // workflow returns cleanly instead of finishing all stages) — so completion
      // is judged from stage_runs reaching the requested stop stage, not from
      // blast_radius_analyses.status.
      const rows = await qx.select(
        `select analysis_id, status from blast_radius_stage_runs
         where analysis_id in ($(ids:csv)) and stage = $(stage)`,
        { ids: analysisIds, stage: STOP_AFTER },
      )
      const finished = rows.filter(
        (r: { status: string }) => r.status === 'succeeded' || r.status === 'failed',
      )

      const sample = sampleContainerStats()
      if (sample) {
        memSamples.push(sample.memUsage)
        console.log(
          `[loadtest] ${finished.length}/${analysisIds.length} finished stage=${STOP_AFTER} ` +
            `mem=${sample.memUsage} cpu=${sample.cpuPerc} (${new Date().toISOString()})`,
        )
      } else {
        console.log(
          `[loadtest] ${finished.length}/${analysisIds.length} finished stage=${STOP_AFTER} ` +
            `(${new Date().toISOString()})`,
        )
      }

      if (finished.length === analysisIds.length) {
        allDone = true
        break
      }
      await new Promise((resolve) => {
        setTimeout(resolve, POLL_INTERVAL_MS)
      })
    }

    if (!allDone) {
      console.warn('[loadtest] TIMED OUT waiting for all analyses to reach the stop stage')
    }

    const stageRuns = await qx.select(
      `select analysis_id, stage, status, duration_ms, cost_usd, error
       from blast_radius_stage_runs where analysis_id in ($(ids:csv))`,
      { ids: analysisIds },
    )

    console.log('\n=== Per-analysis stage outcomes ===')
    for (const id of analysisIds) {
      const runs = stageRuns.filter((r: { analysis_id: string }) => r.analysis_id === id)
      const summary = runs
        .map(
          (r: { stage: string; status: string; error: string | null }) =>
            `${r.stage}=${r.status}${r.error ? ` (${r.error})` : ''}`,
        )
        .join(', ')
      console.log(`${id}: ${summary || 'no stage runs recorded'}`)
    }

    console.log('\n=== Stage duration stats (ms) ===')
    for (const stage of ['intel', 'dependents', 'reachability', 'report']) {
      const durations = stageRuns
        .filter(
          (r: { stage: string; status: string }) => r.stage === stage && r.status === 'succeeded',
        )
        .map((r: { duration_ms: number | string }) => Number(r.duration_ms))
      console.log(`${stage}:`, stats(durations))
    }

    // blast_radius_analyses.total_cost_usd is only ever set by the report stage
    // (finalizeAnalysis) — with stopAfterStage set, report never runs, so the only
    // place real per-stage cost shows up is here, on stage_runs, scoped to this run's
    // own analysisIds (not a global window, since other runs/deployments write here too).
    console.log('\n=== Cost (USD, from blast_radius_stage_runs) ===')
    let totalCost = 0
    for (const stage of ['intel', 'dependents', 'reachability', 'report']) {
      const costs = stageRuns
        .filter((r: { stage: string }) => r.stage === stage)
        .map((r: { cost_usd: number | string | null }) => Number(r.cost_usd ?? 0))
      const stageCost = costs.reduce((sum: number, c: number) => sum + c, 0)
      totalCost += stageCost
      console.log(`${stage}: $${stageCost.toFixed(4)} (${costs.length} runs)`)
    }
    console.log(`total: $${totalCost.toFixed(4)}`)

    if (memSamples.length > 0) {
      console.log(`\n=== Memory samples (docker stats, ${memSamples.length} points) ===`)
      console.log(memSamples.join(' -> '))
    }

    console.log(`\n[loadtest] total wall-clock: ${Date.now() - submittedAt}ms`)
    process.exit(allDone ? 0 : 1)
  } finally {
    resetRunConfig()
  }
}

main().catch((err) => {
  console.error('[loadtest] fatal error', err)
  resetRunConfig()
  process.exit(1)
})
