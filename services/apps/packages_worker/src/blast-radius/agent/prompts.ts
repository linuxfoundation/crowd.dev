// Agent prompts and schemas for Stage 1 (intel) and Stage 3 (reachability).
// Ported from Python PoC (agent/prompts.py).

export interface VulnerableSymbol {
  name: string
  kind: string
  defined_in: string
  exported_as: string[]
  notes?: string
}

export interface SymbolSpec {
  vuln_id: string
  package: string
  summary: string
  vulnerable_symbols: VulnerableSymbol[]
  import_signatures: Record<string, string[]>
  exploit_preconditions: string
  reachability_notes: string
  confidence: number
}

// ---------- STAGE 1: INTEL ----------

export const INTEL_SCHEMA = {
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
      properties: {
        main_then_member: { type: 'array', items: { type: 'string' } },
        deep_import: { type: 'array', items: { type: 'string' } },
        standalone_pkg: { type: 'array', items: { type: 'string' } },
        aliases_and_wrappers: { type: 'array', items: { type: 'string' } },
      },
      required: ['main_then_member', 'deep_import', 'standalone_pkg', 'aliases_and_wrappers'],
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

export const INTEL_SYSTEM_PROMPT = `You are a vulnerability analyst. Your working directory contains the FULL SOURCE of the
vulnerable version of an npm package. You are given the security advisory and the patch
(diff) that fixed the vulnerability.

Your job is to determine, precisely, WHAT is vulnerable — so that downstream analysts can
check whether other packages actually reach the vulnerable code.

Rules:
- Identify the exact vulnerable function(s)/symbol(s) from the patch and the source. Be
  minimal and precise: do NOT include similar-but-unaffected functions. If the patch only
  touches an internal helper, trace which PUBLIC exported functions route through it and
  list those as the reachable surface (note the internal helper in \`notes\`).
- Read the package source to verify how each vulnerable symbol is exported (main entry,
  per-file module paths, re-exports).
- Build \`import_signatures\`: concrete code patterns a JavaScript/TypeScript dependent
  would contain if it uses the vulnerable symbol. Cover: importing the main package then
  accessing the member (CommonJS and ESM), deep/module-path imports, standalone
  per-function npm packages if they exist for this symbol, and common alias/wrapper
  patterns. These are the patterns analysts will grep for — make them literal and
  greppable, not prose.
- \`reachability_notes\` must state what does NOT count (e.g. sibling functions that look
  similar but are not affected) and any conditions required for exploitability.
- Set \`confidence\` for your identification: 0.9+ only if the patch unambiguously
  identifies the symbol(s); lower if you had to infer from indirect evidence.`

export function buildIntelPrompt(
  ovsId: string,
  aliases: string[],
  details: string,
  analyzedVersion: string,
  patches: Record<string, string>,
): string {
  const parts: string[] = []

  parts.push(`# Advisory ${ovsId}`)
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

// ---------- STAGE 3: REACHABILITY ----------

export const VERDICT_SCHEMA = {
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
      enum: ['main-member', 'deep-import', 'standalone-pkg', 'reexport', 'none'],
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

export function buildReachabilitySystemPrompt(spec: SymbolSpec): string {
  const symbolsText = spec.vulnerable_symbols
    .map((s) => {
      const exportedAs = s.exported_as.length > 0 ? s.exported_as.join(', ') : 'n/a'
      const notes = s.notes ? ` — ${s.notes}` : ''
      return `- \`${s.name}\` (${s.kind}, defined in ${s.defined_in}; exported as: ${exportedAs})${notes}`
    })
    .join('\n')

  const signatures = JSON.stringify(spec.import_signatures, null, 2)

  return `You are a security reachability analyst. Your working directory contains the published
source of ONE npm package (the "dependent") that declares a dependency on
\`${spec.package}\`, which has a known vulnerability (${spec.vuln_id}).

## The vulnerability
${spec.summary}

Vulnerable symbol(s) in \`${spec.package}\`:
${symbolsText}

Exploit preconditions: ${spec.exploit_preconditions}

Analyst notes: ${spec.reachability_notes}

## Import signatures to look for
${signatures}

## Your task
Decide whether THIS dependent's own code actually reaches the vulnerable symbol(s).

Scope rules — follow strictly:
1. Only the dependent's OWN shipped code counts. Ignore anything under \`node_modules/\`.
   Usage of the vulnerable symbol inside the dependent's other dependencies is OUT OF
   SCOPE (that is second-level analysis, done separately).
2. Merely importing/depending on \`${spec.package}\` is NOT enough — the vulnerable
   symbol itself must be reached. Uses of other functions from the package are irrelevant.
3. Usage only in test files, examples, benchmarks, or build scripts that are not part of
   the shipped runtime code → \`not_affected\` (explain in reasoning).
4. If the dependent RE-EXPORTS the vulnerable symbol to its own consumers (barrel files,
   wrapper utilities that pass arguments through), that DOES count as \`affected\` with
   \`import_style: "reexport"\` — it propagates the vulnerable surface.
5. Watch for indirect reachability inside the dependent's own code: local wrapper
   functions, aliased imports, destructuring, dynamic member access like \`_[name]\`.
6. \`import_style\` describes how the VULNERABLE SYMBOL is reached, not how the package is
   imported: report \`none\` whenever the vulnerable symbol itself is not reached, even if
   the package is imported for other functions.

Method: grep for the import signatures (and the bare symbol names) across the source,
open every hit, and trace whether the symbol is actually invoked. Check the package's
entry points (package.json main/exports) to understand what ships. Minified/bundled-only
code you cannot confidently interpret → \`unclear\`.

## Confidence calibration
- 0.8–1.0: direct evidence — you found (or ruled out) the import AND the call site
  explicitly; source was readable.
- 0.4–0.8: symbol is imported but the call path is ambiguous (dynamic dispatch,
  conditional use, partial minification).
- <0.4 and/or \`unclear\`: source is minified/bundled/absent, or indirection you could
  not resolve.

Report evidence as exact file paths, line numbers, and short verbatim snippets.`
}

export const REACHABILITY_PROMPT =
  'Analyze this package per your instructions and produce the structured verdict. ' +
  'Start by listing the package structure and grepping for the import signatures.'
