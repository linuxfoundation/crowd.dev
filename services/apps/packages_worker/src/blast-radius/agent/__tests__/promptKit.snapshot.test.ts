import { describe, expect, it } from 'vitest'

import {
  GO_INTEL_SCHEMA,
  GO_INTEL_SYSTEM_PROMPT,
  GO_REACHABILITY_PROMPT,
  GO_VERDICT_SCHEMA,
  buildGoIntelPrompt,
  buildGoReachabilitySystemPrompt,
} from '../goPrompts'
import {
  INTEL_SCHEMA,
  INTEL_SYSTEM_PROMPT,
  REACHABILITY_PROMPT,
  SymbolSpec,
  VERDICT_SCHEMA,
  buildIntelPrompt,
  buildReachabilitySystemPrompt,
} from '../prompts'

// Safety net for the prompts.ts/goPrompts.ts shrink onto promptKit.ts: the assembled
// prompt/schema text must stay byte-identical to what it was before the refactor, since
// prompt wording drives the agent's verdicts. Expected values below are captured verbatim
// from the pre-refactor files.

const NPM_IMPORT_SIGNATURE_KEYS = [
  'main_then_member',
  'deep_import',
  'standalone_pkg',
  'aliases_and_wrappers',
]

const GO_IMPORT_SIGNATURE_KEYS = [
  'plain_import',
  'aliased_import',
  'dot_import',
  'subpackage_import',
]

const NPM_IMPORT_STYLE_ENUM = ['main-member', 'deep-import', 'standalone-pkg', 'reexport', 'none']

const GO_IMPORT_STYLE_ENUM = [
  'plain-import',
  'aliased-import',
  'dot-import',
  'subpackage-import',
  'reexport',
  'none',
]

const SPEC: SymbolSpec = {
  vuln_id: 'GHSA-xxxx-yyyy-zzzz',
  package: 'some-package',
  summary: 'A vulnerability summary.',
  vulnerable_symbols: [
    {
      name: 'vulnerableFn',
      kind: 'function',
      defined_in: 'src/index.js',
      exported_as: ['vulnerableFn', 'default.vulnerableFn'],
      notes: 'some notes',
    },
  ],
  import_signatures: { main_then_member: ['const x = require("pkg"); x.vulnerableFn()'] },
  exploit_preconditions: 'attacker controls input',
  reachability_notes: 'sibling helpers are not affected',
  confidence: 0.9,
}

describe('INTEL_SCHEMA (npm)', () => {
  it('derives import_signatures.properties/required from the npm key list', () => {
    const importSignatures = INTEL_SCHEMA.properties.import_signatures
    expect(Object.keys(importSignatures.properties)).toEqual(NPM_IMPORT_SIGNATURE_KEYS)
    expect(importSignatures.required).toEqual(NPM_IMPORT_SIGNATURE_KEYS)
  })

  it('keeps the top-level schema properties in sync with its required list', () => {
    const propertyKeys = Object.keys(INTEL_SCHEMA.properties).sort()
    const requiredKeys = [...INTEL_SCHEMA.required].sort()
    expect(propertyKeys).toEqual(requiredKeys)
  })
})

describe('GO_INTEL_SCHEMA', () => {
  it('derives import_signatures.properties/required from the go key list', () => {
    const importSignatures = GO_INTEL_SCHEMA.properties.import_signatures
    expect(Object.keys(importSignatures.properties)).toEqual(GO_IMPORT_SIGNATURE_KEYS)
    expect(importSignatures.required).toEqual(GO_IMPORT_SIGNATURE_KEYS)
  })
})

describe('VERDICT_SCHEMA (npm) vs GO_VERDICT_SCHEMA', () => {
  it('injects the ecosystem-specific import_style enum only', () => {
    expect(VERDICT_SCHEMA.properties.import_style.enum).toEqual(NPM_IMPORT_STYLE_ENUM)
    expect(GO_VERDICT_SCHEMA.properties.import_style.enum).toEqual(GO_IMPORT_STYLE_ENUM)
  })
})

describe('INTEL_SYSTEM_PROMPT / GO_INTEL_SYSTEM_PROMPT', () => {
  it('npm prose is unchanged', () => {
    expect(INTEL_SYSTEM_PROMPT).toContain('vulnerable version of an npm package')
    expect(INTEL_SYSTEM_PROMPT).toContain('main entry,\n  per-file module paths, re-exports')
  })

  it('go prose is unchanged', () => {
    expect(GO_INTEL_SYSTEM_PROMPT).toContain('vulnerable version of a Go module')
    expect(GO_INTEL_SYSTEM_PROMPT).toContain(
      'only\n  CapitalCase identifiers are visible outside their package',
    )
  })
})

describe('buildIntelPrompt / buildGoIntelPrompt', () => {
  it('are the same shared builder (byte-identical output)', () => {
    const args: [string, string[], string, string, Record<string, string>] = [
      'GHSA-xxxx',
      ['CVE-2024-0001'],
      'details text',
      '1.2.3',
      { 'fix.patch': '--- a\n+++ b' },
    ]
    expect(buildIntelPrompt(...args)).toBe(buildGoIntelPrompt(...args))
  })

  it('produces the expected assembled text', () => {
    const result = buildIntelPrompt('GHSA-xxxx', [], 'details', '1.2.3', {})
    expect(result).toContain('# Advisory GHSA-xxxx')
    expect(result).toContain(
      'The working directory contains the source of the vulnerable version 1.2.3.',
    )
    expect(result).toContain('No fix patch could be retrieved.')
  })
})

describe('buildReachabilitySystemPrompt / buildGoReachabilitySystemPrompt', () => {
  it('npm prompt keeps npm-specific scope prose and shared symbols block', () => {
    const prompt = buildReachabilitySystemPrompt(SPEC)
    expect(prompt).toContain('published\nsource of ONE npm package')
    expect(prompt).toContain('Ignore anything under `node_modules/`')
    expect(prompt).toContain(
      '- `vulnerableFn` (function, defined in src/index.js; exported as: vulnerableFn, default.vulnerableFn) — some notes',
    )
    expect(prompt).toContain(JSON.stringify(SPEC.import_signatures, null, 2))
  })

  it('go prompt keeps go-specific scope prose and shared symbols block', () => {
    const prompt = buildGoReachabilitySystemPrompt(SPEC)
    expect(prompt).toContain('published\nsource of ONE Go module')
    expect(prompt).toContain('Ignore anything under `vendor/`')
    expect(prompt).toContain(
      '- `vulnerableFn` (function, defined in src/index.js; exported as: vulnerableFn, default.vulnerableFn) — some notes',
    )
    expect(prompt).toContain(JSON.stringify(SPEC.import_signatures, null, 2))
  })
})

describe('REACHABILITY_PROMPT / GO_REACHABILITY_PROMPT', () => {
  it('differ only by package/module wording', () => {
    expect(REACHABILITY_PROMPT).toBe(
      'Analyze this package per your instructions and produce the structured verdict. ' +
        'Start by listing the package structure and grepping for the import signatures.',
    )
    expect(GO_REACHABILITY_PROMPT).toBe(
      'Analyze this module per your instructions and produce the structured verdict. ' +
        'Start by listing the package structure and grepping for the import signatures.',
    )
  })
})
