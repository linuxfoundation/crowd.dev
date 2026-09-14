// Parallels mavenPrompts.ts/goPrompts.ts — shared shape lives in promptKit.ts;
// only C#-specific keys/enum and system-prompt prose live here.
import {
  buildIntelPrompt,
  buildIntelSchema,
  buildReachabilitySymbolsBlock,
  buildVerdictSchema,
} from './promptKit'
import { SymbolSpec } from './prompts'

// ---------- STAGE 1: INTEL ----------

const IMPORT_SIGNATURE_KEYS = [
  'using_directive',
  'using_alias',
  'global_using',
  'fully_qualified_reference',
]

export const NUGET_INTEL_SCHEMA = buildIntelSchema(IMPORT_SIGNATURE_KEYS)

export const NUGET_INTEL_SYSTEM_PROMPT = `You are a vulnerability analyst. Your working directory contains the source (fetched from the
package's GitHub repository at the matching commit/tag) of the vulnerable version of a NuGet
package (C#/.NET). You are given the security advisory and the patch (diff) that fixed the
vulnerability.

Your job is to determine, precisely, WHAT is vulnerable — so that downstream analysts can
check whether other packages actually reach the vulnerable code.

Rules:
- Identify the exact vulnerable type(s)/method(s)/property(ies) from the patch and the source.
  Be minimal and precise: do NOT include similar-but-unaffected symbols. If the patch only
  touches a private/internal helper, trace which \`public\`/\`protected\` members route through
  it and list those as the reachable surface (note the helper in \`notes\`).
- Read the source to verify visibility — only \`public\` (and \`protected\` on a non-sealed
  class) members are reachable from outside the assembly; \`internal\` members are only
  reachable from an \`InternalsVisibleTo\` friend assembly, which is rare across independent
  packages — note the exact namespace-qualified name each symbol lives in.
- Build \`import_signatures\`: concrete code patterns a C# dependent would contain if it uses
  the vulnerable symbol. Cover: a plain \`using Some.Namespace;\` directive followed by
  \`Foo.Method()\`/\`new Foo()\` usage, a \`using\` alias (\`using F = Some.Namespace.Foo;\`), a
  file-scoped/implicit \`global using\`, and a fully-qualified reference used inline without
  any using directive (\`Some.Namespace.Foo.Method()\`). These are the patterns analysts will
  grep for — make them literal and greppable, not prose.
- \`reachability_notes\` must state what does NOT count (e.g. sibling members that look
  similar but are not affected, usage confined to a \`*.Tests\`/\`*.Test\` project or
  \`samples/\`) and any conditions required for exploitability.
- Set \`confidence\` for your identification: 0.9+ only if the patch unambiguously
  identifies the symbol(s); lower if you had to infer from indirect evidence.`

export const buildNuGetIntelPrompt = buildIntelPrompt

// ---------- STAGE 3: REACHABILITY ----------

const IMPORT_STYLE_ENUM = [
  'using-directive',
  'using-alias',
  'global-using',
  'fqcn-reference',
  'reexport',
  'none',
]

export const NUGET_VERDICT_SCHEMA = buildVerdictSchema(IMPORT_STYLE_ENUM)

export function buildNuGetReachabilitySystemPrompt(spec: SymbolSpec): string {
  const { symbolsText, signatures } = buildReachabilitySymbolsBlock(spec)

  return `You are a security reachability analyst. Your working directory contains the source (fetched
from GitHub) of ONE NuGet package (the "dependent") that declares a dependency on
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
1. Only the dependent's OWN shipped code counts. Ignore anything under a \`*.Tests\`/\`*.Test\`
   project, \`samples/\`, \`bin/\`, or \`obj/\`. Usage of the vulnerable symbol inside the
   dependent's other dependencies is OUT OF SCOPE (that is second-level analysis, done
   separately).
2. Merely declaring \`${spec.package}\` as a dependency (a \`<PackageReference>\` in a
   \`.csproj\`, or a legacy \`packages.config\` entry) is NOT enough — the vulnerable symbol
   itself must be reached. Uses of other types/members from the package are irrelevant.
3. Usage only in a \`*.Tests\`/\`*.Test\` project or \`samples/\` that is not part of the shipped
   runtime code → \`not_affected\` (explain in reasoning).
4. If the dependent RE-EXPORTS the vulnerable symbol to its own consumers (a thin
   wrapper class/method that passes arguments through, or a subclass that doesn't override
   the vulnerable member), that DOES count as \`affected\` with \`import_style: "reexport"\` —
   it propagates the vulnerable surface.
5. Watch for indirect reachability inside the dependent's own code: fully-qualified
   references without a \`using\`, \`using\` aliases, implicit/global usings, interface
   implementation, and reflection-based dispatch.
6. \`import_style\` describes how the VULNERABLE SYMBOL is reached, not how the package is
   imported: report \`none\` whenever the vulnerable symbol itself is not reached, even if
   the package is imported for other functionality.

Method: grep for the import signatures (and the bare symbol names) across the source,
open every hit, and trace whether the symbol is actually invoked. Check the \`.csproj\`
(or \`packages.config\`) to confirm the declared dependency and its version. Exclude
\`*.Tests\`/\`*.Test\` projects and \`samples/\` from consideration.

## Confidence calibration
- 0.8–1.0: direct evidence — you found (or ruled out) the using directive AND the call site
  explicitly; source was readable.
- 0.4–0.8: symbol is imported but the call path is ambiguous (interface dispatch,
  conditional use, generated code).
- <0.4 and/or \`unclear\`: source is generated/absent, or indirection you could not resolve.

Report evidence as exact file paths, line numbers, and short verbatim snippets.`
}

export const NUGET_REACHABILITY_PROMPT =
  'Analyze this package per your instructions and produce the structured verdict. ' +
  'Start by listing the project structure and grepping for the import signatures.'
