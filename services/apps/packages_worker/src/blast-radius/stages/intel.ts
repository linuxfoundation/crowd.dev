import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { getEcosystemConfig } from './ecosystems'

// Thin ecosystem dispatcher — bodies live under stages/{npm,go,maven}. Reads the
// ecosystem straight from the analysis row already fetched for this stage, so no extra
// DB round trip beyond what the per-ecosystem body already did before this refactor.
export async function runIntelStage(
  qx: QueryExecutor,
  analysisId: string,
  advisoryOsvId: string,
  onProgress?: () => void,
): Promise<void> {
  const detail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
  const cfg = getEcosystemConfig(detail?.ecosystem)
  return cfg.runIntel(qx, analysisId, advisoryOsvId, onProgress)
}
