import {
  IDbProjectCatalog,
  isSlackBotProvenance,
} from '@crowd/data-access-layer/src/project-catalog/types'

import { MAX_EVALUATION_RUNS } from '../evaluator/retry'

type RetryRow = Pick<IDbProjectCatalog, 'provenance' | 'evaluationAttempts'>

export function canDeferEvaluation(row: RetryRow): boolean {
  return !isSlackBotProvenance(row.provenance) && row.evaluationAttempts + 1 < MAX_EVALUATION_RUNS
}
