// Parallels prompts.ts (npm) — schema shape and the intel prompt builder are shared
// via promptKit.ts; only the Go-specific keys/enum and system-prompt prose live here.
import {
  buildIntelPrompt,
  buildIntelSchema,
  buildReachabilitySymbolsBlock,
  buildVerdictSchema,
} from './promptKit'
import { SymbolSpec } from './prompts'

// ---------- STAGE 1: INTEL ----------

const IMPORT_SIGNATURE_KEYS = ['plain_import', 'aliased_import', 'dot_import', 'subpackage_import']

export const GO_INTEL_SCHEMA = buildIntelSchema(IMPORT_SIGNATURE_KEYS)

export const GO_INTEL_SYSTEM_PROMPT = `You are a vulnerability analyst. Your working directory contains the FULL SOURCE of the
vulnerable version of a Go module. You are given the security advisory and the patch
(diff) that fixed the vulnerability.

Your job is to determine, precisely, WHAT is vulnerable — so that downstream analysts can
check whether other modules actually reach the vulnerable code.

Rules:
- Identify the exact vulnerable function(s)/method(s)/type(s) from the patch and the
  source. Be minimal and precise: do NOT include similar-but-unaffected symbols. If the
  patch only touches an unexported helper, trace which EXPORTED (CapitalCase) symbols
  route through it and list those as the reachable surface (note the helper in \`notes\`).
- Read the module source to verify how each vulnerable symbol is exported — only
  CapitalCase identifiers are visible outside their package; note the exact package path
  each symbol lives in (a module can contain many packages, e.g. \`pkg/foo\`, \`internal/bar\`
  — \`internal/\` packages are never importable outside the module itself).
- Build \`import_signatures\`: concrete code patterns a Go dependent would contain if it
  uses the vulnerable symbol. Cover: a plain \`import "module/path"\` followed by
  \`pkg.Symbol\` usage, an aliased import (\`alias "module/path"\`), a dot import
  (\`. "module/path"\` — symbol used bare), and importing a specific subpackage path
  directly. These are the patterns analysts will grep for — make them literal and
  greppable, not prose.
- \`reachability_notes\` must state what does NOT count (e.g. sibling functions that look
  similar but are not affected, usage confined to \`_test.go\`/\`testdata/\`/\`examples/\`) and
  any conditions required for exploitability.
- Set \`confidence\` for your identification: 0.9+ only if the patch unambiguously
  identifies the symbol(s); lower if you had to infer from indirect evidence.`

export const buildGoIntelPrompt = buildIntelPrompt

// ---------- STAGE 3: REACHABILITY ----------

const IMPORT_STYLE_ENUM = [
  'plain-import',
  'aliased-import',
  'dot-import',
  'subpackage-import',
  'reexport',
  'none',
]

export const GO_VERDICT_SCHEMA = buildVerdictSchema(IMPORT_STYLE_ENUM)

export function buildGoReachabilitySystemPrompt(spec: SymbolSpec): string {
  const { symbolsText, signatures } = buildReachabilitySymbolsBlock(spec)

  return `You are a security reachability analyst. Your working directory contains the published
source of ONE Go module (the "dependent") that declares a dependency on
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
1. Only the dependent's OWN shipped code counts. Ignore anything under \`vendor/\`.
   Usage of the vulnerable symbol inside the dependent's other dependencies is OUT OF
   SCOPE (that is second-level analysis, done separately).
2. Merely importing/depending on \`${spec.package}\` (present in \`go.mod\`) is NOT enough —
   the vulnerable symbol itself must be reached. Uses of other functions from the module
   are irrelevant.
3. Usage only in \`_test.go\` files, \`testdata/\`, or \`examples/\` directories that are not
   part of the shipped runtime code → \`not_affected\` (explain in reasoning).
4. If the dependent RE-EXPORTS the vulnerable symbol to its own consumers (a thin wrapper
   function/type alias that passes arguments through), that DOES count as \`affected\` with
   \`import_style: "reexport"\` — it propagates the vulnerable surface.
5. Watch for indirect reachability inside the dependent's own code: package-qualified
   calls (\`pkg.Func\`), aliased imports, dot imports (bare \`Func\` after \`. "module/path"\`),
   interface embedding, and reflection-based dispatch.
6. \`import_style\` describes how the VULNERABLE SYMBOL is reached, not how the module is
   imported: report \`none\` whenever the vulnerable symbol itself is not reached, even if
   the module is imported for other functions.

Method: grep for the import signatures (and the bare symbol names) across the source,
open every hit, and trace whether the symbol is actually invoked. Check \`go.mod\` to
confirm the declared dependency and its version. Exclude \`vendor/\`, \`_test.go\` files, and
\`testdata/\`/\`examples/\` directories from consideration.

## Confidence calibration
- 0.8–1.0: direct evidence — you found (or ruled out) the import AND the call site
  explicitly; source was readable.
- 0.4–0.8: symbol is imported but the call path is ambiguous (interface dispatch,
  conditional use, generated code).
- <0.4 and/or \`unclear\`: source is generated/vendored-only/absent, or indirection you
  could not resolve.

Report evidence as exact file paths, line numbers, and short verbatim snippets.`
}

export const GO_REACHABILITY_PROMPT =
  'Analyze this module per your instructions and produce the structured verdict. ' +
  'Start by listing the package structure and grepping for the import signatures.'
