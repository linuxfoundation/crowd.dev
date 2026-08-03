import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { goReachabilityConfig } from './go/reachabilityConfig'
import { npmReachabilityConfig } from './npm/reachabilityConfig'
import { runReachabilityStage as runReachabilityStageWithConfig } from './reachabilityStage'

// Thin ecosystem dispatcher — the stage body lives in reachabilityStage.ts, shared by
// every ecosystem; only the small config (prompt/schema/source-resolution) differs.
export async function runReachabilityStage(
  qx: QueryExecutor,
  analysisId: string,
  onProgress?: () => void,
): Promise<void> {
  const detail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
  const cfg = (detail?.ecosystem ?? 'npm') === 'go' ? goReachabilityConfig : npmReachabilityConfig
  return runReachabilityStageWithConfig(qx, analysisId, cfg, onProgress)
}
