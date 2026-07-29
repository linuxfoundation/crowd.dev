export interface ITriggerBlastRadiusAnalysis {
  analysisId: string
  advisoryId: string
  package: string | null
  ecosystem: string
  force: boolean
  // Internal-only, used by the local load-test harness to stop the workflow right
  // after a given stage succeeds (e.g. to profile 'dependents' without ever reaching
  // the paid reachability LLM stage). Never set by the public API.
  stopAfterStage?: 'intel' | 'dependents' | 'reachability'
}
