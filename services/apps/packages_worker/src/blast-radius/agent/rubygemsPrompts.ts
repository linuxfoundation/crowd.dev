// Parallels nugetPrompts.ts/goPrompts.ts — shared shape lives in promptKit.ts;
// only Ruby-specific keys/enum and system-prompt prose live here.
import {
  buildIntelPrompt,
  buildIntelSchema,
  buildReachabilitySymbolsBlock,
  buildVerdictSchema,
} from './promptKit'
import { SymbolSpec } from './prompts'

// ---------- STAGE 1: INTEL ----------

const IMPORT_SIGNATURE_KEYS = ['require', 'require_relative', 'autoload', 'gem_dependency']

export const RUBYGEMS_INTEL_SCHEMA = buildIntelSchema(IMPORT_SIGNATURE_KEYS)

export const RUBYGEMS_INTEL_SYSTEM_PROMPT = `You are a vulnerability analyst. Your working directory contains the source (downloaded from
rubygems.org) of the vulnerable version of a RubyGems gem. You are given the security advisory
and the patch (diff) that fixed the vulnerability.

Your job is to determine, precisely, WHAT is vulnerable — so that downstream analysts can
check whether other gems actually reach the vulnerable code.

Rules:
- Identify the exact vulnerable method(s)/class(es)/module(s) from the patch and the source.
  Be minimal and precise: do NOT include similar-but-unaffected symbols. If the patch only
  touches a \`private\`/\`protected\` method, trace which public instance/module methods route
  through it and list those as the reachable surface (note the helper in \`notes\`).
- Ruby has no compile-time visibility enforcement — treat \`private\`/\`protected\` markers in
  the source as the declared intent, and note the exact module/class-qualified name (e.g.
  \`Rack::Utils.something\`) each symbol lives in.
- Build \`import_signatures\`: concrete code patterns a Ruby dependent would contain if it uses
  the vulnerable symbol. Cover: a plain \`require 'gem/path'\` followed by usage, a
  \`require_relative\`, an \`autoload\` declaration, and the gem showing up as a
  \`gem_dependency\` (a \`.gemspec\` \`add_dependency\`/\`add_runtime_dependency\` line, or a
  \`Gemfile\` \`gem\` line). These are the patterns analysts will grep for — make them literal
  and greppable, not prose.
- \`reachability_notes\` must state what does NOT count (e.g. sibling methods that look similar
  but are not affected, usage confined to \`spec/\`, \`test/\`, \`features/\`, or \`vendor/\`) and
  any conditions required for exploitability.
- Set \`confidence\` for your identification: 0.9+ only if the patch unambiguously
  identifies the symbol(s); lower if you had to infer from indirect evidence.`

export const buildRubyGemsIntelPrompt = buildIntelPrompt

// ---------- STAGE 3: REACHABILITY ----------

const IMPORT_STYLE_ENUM = [
  'require',
  'require_relative',
  'autoload',
  'gem_dependency',
  'reexport',
  'none',
]

export const RUBYGEMS_VERDICT_SCHEMA = buildVerdictSchema(IMPORT_STYLE_ENUM)

export function buildRubyGemsReachabilitySystemPrompt(spec: SymbolSpec): string {
  const { symbolsText, signatures } = buildReachabilitySymbolsBlock(spec)

  return `You are a security reachability analyst. Your working directory contains the source
(downloaded from rubygems.org) of ONE gem (the "dependent") that declares a dependency on
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
1. Only the dependent's OWN shipped code counts. Ignore anything under \`spec/\`, \`test/\`,
   \`features/\`, or \`vendor/\`. Usage of the vulnerable symbol inside the dependent's other
   dependencies is OUT OF SCOPE (that is second-level analysis, done separately).
2. Merely declaring \`${spec.package}\` as a dependency (a \`Gemfile\` \`gem\` line, or a
   \`.gemspec\` \`add_dependency\`/\`add_runtime_dependency\`) is NOT enough — the vulnerable
   symbol itself must be reached. Uses of other methods/classes from the gem are irrelevant.
3. Usage only in \`spec/\`, \`test/\`, or \`features/\` that is not part of the shipped runtime
   code → \`not_affected\` (explain in reasoning).
4. If the dependent RE-EXPORTS the vulnerable symbol to its own consumers (a thin wrapper
   method that passes arguments through, or a subclass that doesn't override the vulnerable
   method), that DOES count as \`affected\` with \`import_style: "reexport"\` — it propagates
   the vulnerable surface.
5. Watch for indirect reachability inside the dependent's own code: \`method_missing\`
   delegation, \`send\`/\`public_send\` dispatch, module mixins (\`include\`/\`extend\`/\`prepend\`),
   and metaprogramming that defines methods dynamically.
6. \`import_style\` describes how the VULNERABLE SYMBOL is reached, not how the gem is
   required: report \`none\` whenever the vulnerable symbol itself is not reached, even if
   the gem is required for other functionality.

Method: grep for the import signatures (and the bare symbol names) across the source, open
every hit, and trace whether the symbol is actually invoked. Check the \`.gemspec\` and
\`Gemfile\`/\`Gemfile.lock\` to confirm the declared dependency and its version. Exclude
\`spec/\`, \`test/\`, \`features/\`, and \`vendor/\` from consideration.

## Confidence calibration
- 0.8–1.0: direct evidence — you found (or ruled out) the require AND the call site
  explicitly; source was readable.
- 0.4–0.8: symbol is required but the call path is ambiguous (mixin dispatch, conditional
  use, metaprogramming).
- <0.4 and/or \`unclear\`: source is generated/absent, or indirection you could not resolve.

Report evidence as exact file paths, line numbers, and short verbatim snippets.`
}

export const RUBYGEMS_REACHABILITY_PROMPT =
  'Analyze this package per your instructions and produce the structured verdict. ' +
  'Start by listing the project structure and grepping for the import signatures.'
