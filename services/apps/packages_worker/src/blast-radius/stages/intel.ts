import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { runIntelStageGo } from './go/intelGo'
import { runIntelStageNpm } from './npm/intelNpm'

// Thin ecosystem dispatcher — bodies live under stages/npm and stages/go. Reads the
// ecosystem straight from the analysis row already fetched for this stage, so no extra
// DB round trip beyond what runIntelStageNpm already did before this refactor.
export async function runIntelStage(
  qx: QueryExecutor,
  analysisId: string,
  advisoryOsvId: string,
  onProgress?: () => void,
): Promise<void> {
  const detail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
  if ((detail?.ecosystem ?? 'npm') === 'go') {
    return runIntelStageGo(qx, analysisId, advisoryOsvId, onProgress)
  }
  return runIntelStageNpm(qx, analysisId, advisoryOsvId, onProgress)
}
