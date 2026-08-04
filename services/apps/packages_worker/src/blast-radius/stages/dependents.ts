import * as blastRadiusDal from '@crowd/data-access-layer/src/packages/blastRadius'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { runDependentsStageGo } from './go/dependentsGo'
import { runDependentsStageNpm } from './npm/dependentsNpm'

export async function runDependentsStage(
  qx: QueryExecutor,
  analysisId: string,
  onProgress?: () => void,
  signal?: AbortSignal,
): Promise<void> {
  const detail = await blastRadiusDal.getAnalysisDetail(qx, analysisId)
  if ((detail?.ecosystem ?? 'npm') === 'go') {
    return runDependentsStageGo(qx, analysisId, onProgress, signal)
  }
  return runDependentsStageNpm(qx, analysisId, onProgress, signal)
}
