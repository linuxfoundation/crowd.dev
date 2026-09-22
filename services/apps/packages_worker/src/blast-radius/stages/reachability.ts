import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { getEcosystemConfig } from './ecosystems'
import { runReachabilityStage as runReachabilityStageWithConfig } from './reachabilityStage'

// Thin ecosystem dispatcher — the stage body lives in reachabilityStage.ts, shared by
// every ecosystem; only the small config (prompt/schema/source-resolution) differs.
export async function runReachabilityStage(
  qx: QueryExecutor,
  analysisId: string,
  onProgress?: () => void,
): Promise<void> {
  const detail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
  const cfg = getEcosystemConfig(detail?.ecosystem)
  return runReachabilityStageWithConfig(qx, analysisId, cfg.reachability, onProgress)
}
