import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { getEcosystemConfig } from './ecosystems'

export async function runDependentsStage(
  qx: QueryExecutor,
  analysisId: string,
  onProgress?: () => void,
  signal?: AbortSignal,
): Promise<void> {
  const detail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
  const cfg = getEcosystemConfig(detail?.ecosystem)
  return cfg.runDependents(qx, analysisId, onProgress, signal)
}
