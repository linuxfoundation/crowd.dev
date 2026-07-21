export interface ITriggerBlastRadiusAnalysis {
  analysisId: string
  advisoryId: string
  package: string | null
  ecosystem: string | null
  force: boolean
}
