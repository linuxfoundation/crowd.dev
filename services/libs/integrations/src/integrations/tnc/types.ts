import { IActivityScoringGrid } from '@crowd/types'

export enum TncActivityType {
  ENROLLED_CERTIFICATION = 'enrolled-certification',
  ENROLLED_TRAINING = 'enrolled-training',
  ISSUED_CERTIFICATION = 'issued-certification',
  ATTEMPTED_COURSE = 'attempted-course',
  ATTEMPTED_EXAM = 'attempted-exam',
}

export const TNC_GRID: Record<TncActivityType, IActivityScoringGrid> = {
  [TncActivityType.ENROLLED_CERTIFICATION]: { score: 1 },
  [TncActivityType.ENROLLED_TRAINING]: { score: 1 },
  [TncActivityType.ISSUED_CERTIFICATION]: { score: 1 },
  [TncActivityType.ATTEMPTED_COURSE]: { score: 1 },
  [TncActivityType.ATTEMPTED_EXAM]: { score: 1 },
}
