export interface IDiscoverySourceState {
  source: string
  watermark: string | null
  lastRunAt: string | null
}

// Page-based resume cursor for sources whose corpus is periodically re-ranked
// (e.g. lf-criticality-score), where a scalar time watermark doesn't fit.
export interface IDiscoverySourceCursor {
  rundate: string
  page: number
}
