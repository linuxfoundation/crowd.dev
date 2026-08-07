// Parallels cargoPrompts.ts — schema shape and the intel prompt builder are shared via
// promptKit.ts; only the Python-specific keys/enum and system-prompt prose live here.
import {
  buildIntelPrompt,
  buildIntelSchema,
  buildReachabilitySymbolsBlock,
  buildVerdictSchema,
} from './promptKit'
import { SymbolSpec } from './prompts'

const IMPORT_SIGNATURE_KEYS = ['import_module', 'from_import', 'attribute_access', 'dynamic_import']

export const PYPI_INTEL_SCHEMA = buildIntelSchema(IMPORT_SIGNATURE_KEYS)

export const PYPI_INTEL_SYSTEM_PROMPT = `You are a vulnerability analyst. Your working directory contains the FULL SOURCE of the
vulnerable version of a Python package. You are given the security advisory and the patch
(diff) that fixed the vulnerability.

Your job is to determine, precisely, WHAT is vulnerable — so that downstream analysts can
check whether other packages actually reach the vulnerable code.

Rules:
- Identify the exact vulnerable function(s)/method(s)/class(es) from the patch and the
  source. Be minimal and precise: do NOT include similar-but-unaffected symbols. If the
  patch only touches a private (\`_\`-prefixed) helper, trace which public symbols route
  through it and list those as the reachable surface (note the helper in \`notes\`).
- Python has no compile-time visibility — determine what's actually importable by a
  dependent: check \`__all__\` in the defining module's \`__init__.py\` (if present, only
  names listed there are the package's public API), the \`_\`-prefix convention for private
  names, and whether the symbol is re-exported through a package's \`__init__.py\` (which
  counts as \`reexport\`). Note the exact module path each symbol lives in (e.g.
  \`package.submodule.Symbol\`).
- Build \`import_signatures\`: concrete code patterns a dependent package would contain if
  it uses the vulnerable symbol. Cover: \`import package.module\` followed by fully-qualified
  attribute access, \`from package.module import Symbol\` followed by bare \`Symbol\` usage,
  attribute access on an imported module/class instance, and dynamic import via
  \`importlib.import_module(...)\` or \`__import__(...)\`. These are the patterns analysts will
  grep for — make them literal and greppable, not prose.
- \`reachability_notes\` must state what does NOT count (e.g. sibling functions that look
  similar but are not affected, usage confined to \`tests/\`, \`test/\`, \`docs/\`, or
  \`examples/\`) and any conditions required for exploitability (e.g. an optional extra must
  be installed, or a specific argument/config must be set).
- Set \`confidence\` for your identification: 0.9+ only if the patch unambiguously
  identifies the symbol(s); lower if you had to infer from indirect evidence.`

export const buildPyPiIntelPrompt = buildIntelPrompt

const IMPORT_STYLE_ENUM = [
  'import-module',
  'from-import',
  'attribute-access',
  'dynamic-import',
  'reexport',
  'none',
]

export const PYPI_VERDICT_SCHEMA = buildVerdictSchema(IMPORT_STYLE_ENUM)

export function buildPyPiReachabilitySystemPrompt(spec: SymbolSpec): string {
  const { symbolsText, signatures } = buildReachabilitySymbolsBlock(spec)

  return `You are a security reachability analyst. Your working directory contains the published
source of ONE Python package (the "dependent") that declares a dependency on
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
1. Only the dependent's OWN shipped code counts (its package source, not vendored/third-
   party code bundled inside it). Usage of the vulnerable symbol inside the dependent's
   OTHER dependencies (its own \`pyproject.toml\`/\`setup.py\`/\`requirements*.txt\` deps) is
   OUT OF SCOPE (that is second-level analysis, done separately).
2. Merely declaring a dependency on \`${spec.package}\` (present in \`pyproject.toml\`,
   \`setup.py\`, or a \`requirements*.txt\`) is NOT enough — the vulnerable symbol itself must
   be reached. Uses of other items from the package are irrelevant.
3. Usage only in \`tests/\`, \`test/\`, \`docs/\`, or \`examples/\` that is not part of the
   shipped runtime code → \`not_affected\` (explain in reasoning).
4. If the dependent RE-EXPORTS the vulnerable symbol to its own consumers (via its
   \`__init__.py\`, or a thin wrapper function/class that passes arguments through), that
   DOES count as \`affected\` with \`import_style: "reexport"\` — it propagates the
   vulnerable surface.
5. Watch for indirect reachability inside the dependent's own code: fully-qualified
   attribute access (\`package.module.Symbol\`), subclassing the vulnerable class, and
   dynamic import via \`importlib.import_module\` or \`__import__\`.
6. \`import_style\` describes how the VULNERABLE SYMBOL is reached, not how the package is
   declared: report \`none\` whenever the vulnerable symbol itself is not reached, even if
   the package is a dependency for other functionality.

Method: grep for the import signatures (and the bare symbol names) across the source, open
every hit, and trace whether the symbol is actually invoked. Check \`pyproject.toml\`,
\`setup.py\`, or \`requirements*.txt\` to confirm the declared dependency, its version
requirement, and whether an optional extra gates the vulnerable code path. Exclude
\`tests/\`, \`test/\`, \`docs/\`, and \`examples/\` from consideration.

## Confidence calibration
- 0.8–1.0: direct evidence — you found (or ruled out) the import AND the call site
  explicitly; source was readable.
- 0.4–0.8: symbol is imported but the call path is ambiguous (dynamic dispatch, conditional
  imports, generated code).
- <0.4 and/or \`unclear\`: source is generated/absent, or indirection you could not resolve.

Report evidence as exact file paths, line numbers, and short verbatim snippets.`
}

export const PYPI_REACHABILITY_PROMPT =
  'Analyze this package per your instructions and produce the structured verdict. ' +
  'Start by listing the package structure and grepping for the import signatures.'
