export interface ScorecardPageCounts {
  fetched: number
  scored: number
  notFound: number
  failed: number
  unmapped: number
}

export function emptyScorecardCounts(): ScorecardPageCounts {
  return { fetched: 0, scored: 0, notFound: 0, failed: 0, unmapped: 0 }
}

export function addScorecardCounts(
  total: ScorecardPageCounts,
  page: ScorecardPageCounts,
): ScorecardPageCounts {
  return {
    fetched: total.fetched + page.fetched,
    scored: total.scored + page.scored,
    notFound: total.notFound + page.notFound,
    failed: total.failed + page.failed,
    unmapped: total.unmapped + page.unmapped,
  }
}
