export interface IDiscoverySourceState {
  source: string
  watermark: string | null
  lastRunAt: string | null
}

// Page-based resume cursor for sources whose corpus is periodically re-ranked
// (e.g. lf-criticality-score), where a scalar time watermark can't express "resume
// paging through this same ranking" vs "the ranking changed, start over".
export interface IDiscoverySourceCursor {
  rundate: string
  page: number
}
