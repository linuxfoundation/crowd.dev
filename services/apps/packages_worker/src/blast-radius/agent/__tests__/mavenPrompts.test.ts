import { describe, expect, it } from 'vitest'

import { MAVEN_INTEL_SCHEMA } from '../mavenPrompts'

describe('MAVEN_INTEL_SCHEMA', () => {
  it('keeps import_signatures.properties keys in sync with its required list', () => {
    const importSignatures = MAVEN_INTEL_SCHEMA.properties.import_signatures
    const propertyKeys = Object.keys(importSignatures.properties).sort()
    const requiredKeys = [...importSignatures.required].sort()
    expect(propertyKeys).toEqual(requiredKeys)
  })

  it('keeps the top-level schema properties in sync with its required list', () => {
    const propertyKeys = Object.keys(MAVEN_INTEL_SCHEMA.properties).sort()
    const requiredKeys = [...MAVEN_INTEL_SCHEMA.required].sort()
    expect(propertyKeys).toEqual(requiredKeys)
  })
})
