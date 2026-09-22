import { describe, expect, it } from 'vitest'

import { GO_INTEL_SCHEMA } from '../goPrompts'

describe('GO_INTEL_SCHEMA', () => {
  it('keeps import_signatures.properties keys in sync with its required list', () => {
    const importSignatures = GO_INTEL_SCHEMA.properties.import_signatures
    const propertyKeys = Object.keys(importSignatures.properties).sort()
    const requiredKeys = [...importSignatures.required].sort()
    expect(propertyKeys).toEqual(requiredKeys)
  })

  it('keeps the top-level schema properties in sync with its required list', () => {
    const propertyKeys = Object.keys(GO_INTEL_SCHEMA.properties).sort()
    const requiredKeys = [...GO_INTEL_SCHEMA.required].sort()
    expect(propertyKeys).toEqual(requiredKeys)
  })
})
