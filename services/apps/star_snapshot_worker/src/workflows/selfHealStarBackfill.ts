import {
  ParentClosePolicy,
  WorkflowIdReusePolicy,
  continueAsNew,
  log,
  patched,
  proxyActivities,
  startChild,
  workflowInfo,
} from '@temporalio/workflow'

import * as activities from '../activities'
import { backfillStarHistoryBatch } from './backfillStarHistoryBatch'

const { findReposNeedingStarBackfill, findReposNeedingGapHeal } = proxyActivities<
  typeof activities
>({
  startToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3, backoffCoefficient: 2 },
})

const PAGE_SIZE = 1_000
const BATCH_SIZE = 100

export interface ISelfHealStarBackfillArgs {
  afterUrl?: string
  mainScanDone?: boolean
  gapHealAfterUrl?: string
  gapHealDone?: boolean
  batchesDispatchedSoFar?: number
}

// Workflow isolate only allows 'assert'/'url'/'util' Node builtins - crypto is stubbed out,
// so this hash is plain JS (FNV-1a) instead of createHash.
function fnv1a32Hex(input: string): string {
  let hash = 0x811c9dc5
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i)
    hash = Math.imul(hash, 0x01000193)
  }
  return (hash >>> 0).toString(16).padStart(8, '0')
}

// Content-addressed (not positional) so a workflow retry with a reshuffled candidate list can't
// collide two different batches under REJECT_DUPLICATE and silently skip one. Namespaced by scan
// kind so a gap-heal batch can never collide with a main-scan batch under REJECT_DUPLICATE and
// get silently skipped as an already-started duplicate. The main-scan ID format is already live,
// so `namespaced` (gated by patched()) keeps an in-flight main-scan batch dispatched under the
// old, un-namespaced format from getting a different-looking ID on replay.
function batchWorkflowId(
  scanKind: 'main' | 'gap-heal',
  batch: Awaited<ReturnType<typeof findReposNeedingStarBackfill>>,
  namespaced: boolean,
): string {
  const digest = fnv1a32Hex(
    batch
      .map((repo) => repo.repositoryId)
      .sort()
      .join(','),
  )
  return namespaced
    ? `${workflowInfo().workflowId}/${scanKind}-batch-${digest}`
    : `${workflowInfo().workflowId}/batch-${digest}`
}

async function startBatchChild(
  batch: Awaited<ReturnType<typeof findReposNeedingStarBackfill>>,
  workflowId: string,
): Promise<void> {
  try {
    await startChild(backfillStarHistoryBatch, {
      workflowId,
      workflowIdReusePolicy: WorkflowIdReusePolicy.REJECT_DUPLICATE,
      parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON,
      args: [{ repos: batch }],
    })
  } catch (err) {
    if (!(err instanceof Error) || err.name !== 'WorkflowExecutionAlreadyStartedError') {
      throw err
    }
    log.warn(
      'batch child already started for this id, skipping (likely a retry of this workflow)',
      {
        workflowId,
      },
    )
  }
}

// Fans out to abandoned child workflows so each batch's own rate-limit retry loop doesn't
// hold up other pages. Runs two independently-cursored paged scans per tick.
export async function selfHealStarBackfill(args: ISelfHealStarBackfillArgs = {}): Promise<void> {
  let batchesDispatched = args.batchesDispatchedSoFar ?? 0
  const mainScanDone = args.mainScanDone ?? false
  const gapHealDone = args.gapHealDone ?? false

  // patched() keeps a main-scan batch already dispatched in this run's history on its old,
  // un-namespaced child-workflow ID so a replay after this deploy doesn't compute a
  // different-looking ID for that call and hit a nondeterminism error.
  const namespacedBatchIds = patched('CM-1441-namespaced-batch-ids')

  let repos: Awaited<ReturnType<typeof findReposNeedingStarBackfill>> = []
  if (!mainScanDone) {
    repos = await findReposNeedingStarBackfill(PAGE_SIZE, args.afterUrl)
    for (let i = 0; i < repos.length; i += BATCH_SIZE) {
      const batch = repos.slice(i, i + BATCH_SIZE)
      await startBatchChild(batch, batchWorkflowId('main', batch, namespacedBatchIds))
      batchesDispatched++
    }
  }

  // patched() keeps an execution already in flight on its old command sequence so a
  // mid-deploy replay doesn't hit a nondeterminism error.
  const gapHealPatched = patched('CM-1441-gap-heal-scan')
  let gapHealPage: Awaited<ReturnType<typeof findReposNeedingGapHeal>> | undefined
  if (!gapHealDone && gapHealPatched) {
    gapHealPage = await findReposNeedingGapHeal(PAGE_SIZE, args.gapHealAfterUrl)
    for (let i = 0; i < gapHealPage.gappedRepos.length; i += BATCH_SIZE) {
      const batch = gapHealPage.gappedRepos.slice(i, i + BATCH_SIZE)
      // Gap-heal batches are a brand-new call site introduced by this PR (no prior deployed
      // format to preserve), so they're always namespaced.
      await startBatchChild(batch, batchWorkflowId('gap-heal', batch, true))
      batchesDispatched++
    }
  }

  const nextMainScanDone = mainScanDone || repos.length < PAGE_SIZE
  // Only advance gapHealDone once the scan actually ran (gapHealPatched) - otherwise a
  // pre-deploy replay would bake gapHealDone: true into continueAsNew and never run it.
  const nextGapHealDone =
    gapHealDone || (gapHealPatched && (gapHealPage?.pageSize ?? 0) < PAGE_SIZE)
  // While unpatched, gap healing doesn't exist yet - the continue/complete decision must
  // depend on nextMainScanDone alone, exactly like the pre-gap-heal command sequence.
  const shouldContinue = gapHealPatched ? !nextMainScanDone || !nextGapHealDone : !nextMainScanDone

  if (shouldContinue) {
    await continueAsNew<typeof selfHealStarBackfill>({
      afterUrl: nextMainScanDone ? undefined : repos[repos.length - 1].repoUrl,
      mainScanDone: nextMainScanDone,
      gapHealAfterUrl: nextGapHealDone ? undefined : (gapHealPage?.lastUrl ?? args.gapHealAfterUrl),
      gapHealDone: nextGapHealDone,
      batchesDispatchedSoFar: batchesDispatched,
    })
    return
  }

  log.info('selfHealStarBackfill complete', { batchesDispatched })
}
