import { describe, expect, it } from 'vitest'

import { credlyTemplateOverrides } from './overrides'

describe('credlyTemplateOverrides', () => {
  it('lists every template once', () => {
    const ids = credlyTemplateOverrides.map((override) => override.badgeTemplateId)
    expect(new Set(ids).size).toBe(ids.length)
  })

  it('records who decided and where', () => {
    expect(credlyTemplateOverrides.every((override) => override.reviewedBy && override.note)).toBe(
      true,
    )
  })

  it('maps every reviewed template to a project', () => {
    expect(
      credlyTemplateOverrides.every(
        (override) => override.status === 'mapped' && override.projectSlug.length > 0,
      ),
    ).toBe(true)
  })
})
