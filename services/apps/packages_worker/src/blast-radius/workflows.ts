import { log } from '@temporalio/workflow'

import type { ITriggerBlastRadiusAnalysis } from '@crowd/types'

import { buildEcosystemNotSupportedFailure } from './ecosystemSupport'

// 2a's on-demand trigger (see submitBlastRadiusJob in the backend akrites-external
// API). The reachability pipeline isn't built yet, so every request fails with a
// non-retryable "ecosystem not supported" error — see ecosystemSupport.ts. No
// proxyActivities yet: nothing here calls out to an activity.
export async function analyzeBlastRadius(input: ITriggerBlastRadiusAnalysis): Promise<void> {
  log.info('analyzeBlastRadius received', { ...input })

  throw buildEcosystemNotSupportedFailure(input.ecosystem)
}
