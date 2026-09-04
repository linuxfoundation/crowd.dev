import { describe, expect, it } from 'vitest'

import { buildInsightsProjectSkipReason } from './insightsProjectSkip'

describe('buildInsightsProjectSkipReason', () => {
  it('normalizes the project slug and includes the deletion date', () => {
    const reason = buildInsightsProjectSkipReason(
      'nonlf_gerritcodereview-gerrit',
      '2026-04-10T00:00:00.000Z',
    )

    expect(reason).toBe(
      "Insights project 'nonlf-gerritcodereview-gerrit' was deleted on 2026-04-10T00:00:00.000Z; onboarding skipped for manual review",
    )
  })
})
