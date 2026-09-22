export type CheckStatus = 'pass' | 'warn' | 'fail' | 'skip' | 'error'

export interface ICheckResult {
  id: string
  category: string
  status: CheckStatus
  message: string
  details?: Record<string, unknown>
}
