// Mechanically-identical builders shared across ecosystem prompt modules; system-prompt
// PROSE stays per-ecosystem since scope rules genuinely differ and drive LLM behavior.
import type { SymbolSpec, VulnerableSymbol } from './prompts'

export function buildIntelSchema(importSignatureKeys: string[]) {
  return {
    type: 'object',
    properties: {
      summary: {
        type: 'string',
        description: '1-2 sentence plain-language summary of the vulnerability',
      },
      vulnerable_symbols: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            name: { type: 'string' },
            kind: { type: 'string' },
            defined_in: { type: 'string' },
            exported_as: { type: 'array', items: { type: 'string' } },
            notes: { type: 'string' },
          },
          required: ['name', 'kind', 'defined_in', 'exported_as'],
        },
      },
      import_signatures: {
        type: 'object',
        description:
          "Every way a dependent's code could import/reach the vulnerable symbol(s), grouped by style",
        properties: Object.fromEntries(
          importSignatureKeys.map((key) => [key, { type: 'array', items: { type: 'string' } }]),
        ),
        // Derived from the same key list as `properties` above — cannot drift out of sync.
        required: importSignatureKeys,
      },
      exploit_preconditions: { type: 'string' },
      reachability_notes: {
        type: 'string',
        description:
          'Guidance for the reachability analysts: what counts as reaching the vulnerability, and what explicitly does NOT',
      },
      confidence: { type: 'number', minimum: 0, maximum: 1 },
    },
    required: [
      'summary',
      'vulnerable_symbols',
      'import_signatures',
      'exploit_preconditions',
      'reachability_notes',
      'confidence',
    ],
  }
}

export function buildVerdictSchema(importStyleEnum: string[]) {
  return {
    type: 'object',
    properties: {
      uses_package: {
        type: 'boolean',
        description:
          "Does the dependent's own code reference the vulnerable package (or a standalone per-function variant) at all?",
      },
      imports_vulnerable_symbol: { type: 'boolean' },
      import_style: {
        type: 'string',
        enum: importStyleEnum,
        description:
          "How the VULNERABLE SYMBOL itself is reached — 'none' whenever the vulnerable symbol is not imported/reached, even if the package is imported for other functions",
      },
      reachable_verdict: {
        type: 'string',
        enum: ['affected', 'not_affected', 'unclear'],
      },
      confidence: { type: 'number', minimum: 0, maximum: 1 },
      evidence: {
        type: 'array',
        items: {
          type: 'object',
          properties: {
            file: { type: 'string' },
            line: { type: 'integer' },
            snippet: { type: 'string' },
          },
          required: ['file', 'line', 'snippet'],
        },
      },
      reasoning: { type: 'string' },
    },
    required: [
      'uses_package',
      'imports_vulnerable_symbol',
      'import_style',
      'reachable_verdict',
      'confidence',
      'evidence',
      'reasoning',
    ],
  }
}

export function buildIntelPrompt(
  osvId: string,
  aliases: string[],
  details: string,
  analyzedVersion: string,
  patches: Record<string, string>,
): string {
  const parts: string[] = []

  parts.push(`# Advisory ${osvId}`)
  if (aliases.length > 0) {
    parts.push(`Aliases: ${aliases.join(', ')}`)
  }

  parts.push(`\n## Advisory text\n${details.slice(0, 8000)}`)
  parts.push(
    `\nThe working directory contains the source of the vulnerable version ${analyzedVersion}.`,
  )

  if (Object.keys(patches).length > 0) {
    parts.push('\n## Fix patch(es)')
    const budget = Math.floor(40000 / Math.max(Object.keys(patches).length, 1))
    for (const [slug, text] of Object.entries(patches)) {
      parts.push(`\n### ${slug}\n\`\`\`diff\n${text.slice(0, budget)}\n\`\`\``)
    }
  } else {
    parts.push(
      '\nNo fix patch could be retrieved. Derive the vulnerable symbols from the ' +
        'advisory text and the package source alone, and lower your confidence accordingly.',
    )
  }

  parts.push(
    '\nAnalyze the vulnerability and produce the structured output. ' +
      'Verify export paths against the actual source files before answering.',
  )

  return parts.join('\n')
}

// Maps blastRadiusDal.getSymbolSpec's raw DB row (JSONB columns as unknown) to the
// shape the reachability prompt builder reads — shared by every ecosystem's stage.
export function toPromptSymbolSpec(row: Record<string, unknown>): SymbolSpec {
  return {
    vuln_id: String(row.vuln_id ?? ''),
    package: String(row.package ?? ''),
    summary: String(row.summary ?? ''),
    vulnerable_symbols: (row.vulnerable_symbols ?? []) as SymbolSpec['vulnerable_symbols'],
    import_signatures: (row.import_signatures ?? {}) as SymbolSpec['import_signatures'],
    exploit_preconditions: String(row.exploit_preconditions ?? ''),
    reachability_notes: String(row.reachability_notes ?? ''),
    confidence: Number(row.confidence ?? 0),
  }
}

// The identical head of every ecosystem's reachability system prompt (symbol list +
// import signatures JSON) — each ecosystem's builder prepends/appends its own prose.
export function buildReachabilitySymbolsBlock(spec: SymbolSpec): {
  symbolsText: string
  signatures: string
} {
  const symbolsText = spec.vulnerable_symbols
    .map((s: VulnerableSymbol) => {
      const exportedAs = s.exported_as.length > 0 ? s.exported_as.join(', ') : 'n/a'
      const notes = s.notes ? ` — ${s.notes}` : ''
      return `- \`${s.name}\` (${s.kind}, defined in ${s.defined_in}; exported as: ${exportedAs})${notes}`
    })
    .join('\n')

  const signatures = JSON.stringify(spec.import_signatures, null, 2)

  return { symbolsText, signatures }
}
