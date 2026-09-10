import { describe, expect, it } from 'vitest'

import { goConstraintMayInclude } from '../goConstraint'

describe('goConstraintMayInclude', () => {
  it('matches when the floor is below the max vulnerable version', () => {
    expect(goConstraintMayInclude('v1.0.0', 'v1.5.0')).toBe('matched')
  })

  it('matches when the floor equals the max vulnerable version', () => {
    expect(goConstraintMayInclude('v1.5.0', 'v1.5.0')).toBe('matched')
  })

  it('excludes when the floor is above the max vulnerable version', () => {
    expect(goConstraintMayInclude('v2.0.0', 'v1.5.0')).toBe('excluded')
  })

  it('matches pseudo-version floors below the max vulnerable version', () => {
    expect(goConstraintMayInclude('v0.0.0-20200101000000-abcdef123456', 'v1.5.0')).toBe('matched')
  })

  it('conservatively includes an unparseable floor', () => {
    expect(goConstraintMayInclude('not-a-version', 'v1.5.0')).toBe('unparseable-included')
  })
})
