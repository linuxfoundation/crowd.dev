import { describe, expect, it } from 'vitest'

import { buildInsightsProjectSkipReason, needsDeletedProjectCheck } from './insightsProjectSkip'

describe('buildInsightsProjectSkipReason', () => {
  it('names the deletion day without exposing the project slug', () => {
    expect(buildInsightsProjectSkipReason('2026-04-10T00:00:00.000Z')).toBe(
      'its LFX Insights project was deleted on 2026-04-10, so the CDP team has to look at it',
    )
  })
})

describe('needsDeletedProjectCheck', () => {
  it.each(['create_integration', 'update_integration'] as const)(
    'skips the check when the row joins an existing project (%s)',
    (proposedAction) => {
      expect(needsDeletedProjectCheck({ proposedAction })).toBe(false)
    },
  )

  it.each(['create_segment', null] as const)(
    'keeps the check when the row may create a project (%s)',
    (proposedAction) => {
      expect(needsDeletedProjectCheck({ proposedAction })).toBe(true)
    },
  )
})
