import { describe, expect, it } from 'vitest'

import { CARGO_INTEL_SCHEMA } from '../cargoPrompts'

describe('CARGO_INTEL_SCHEMA', () => {
  it('keeps import_signatures.properties keys in sync with its required list', () => {
    const importSignatures = CARGO_INTEL_SCHEMA.properties.import_signatures
    const propertyKeys = Object.keys(importSignatures.properties).sort()
    const requiredKeys = [...importSignatures.required].sort()
    expect(propertyKeys).toEqual(requiredKeys)
  })

  it('keeps the top-level schema properties in sync with its required list', () => {
    const propertyKeys = Object.keys(CARGO_INTEL_SCHEMA.properties).sort()
    const requiredKeys = [...CARGO_INTEL_SCHEMA.required].sort()
    expect(propertyKeys).toEqual(requiredKeys)
  })
})
