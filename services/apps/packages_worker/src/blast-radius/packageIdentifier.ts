// The blast-radius submit endpoint accepts either a bare npm package name
// ("lodash", "@babel/core") or a full purl ("pkg:npm/lodash", "pkg:npm/%40babel/core@4.17.21")
// for the `package` field — see blastRadiusJobRequestSchema. OSV affected-package entries and
// the npm registry only ever use bare names, so a purl must be reduced to that form before
// it's compared against them (raw string equality otherwise never matches a purl input).
export function toBareNpmName(input: string): string {
  let name = input.trim()

  const q = name.indexOf('?')
  const h = name.indexOf('#')
  const cut = q === -1 ? h : h === -1 ? q : Math.min(q, h)
  if (cut !== -1) name = name.slice(0, cut)

  if (name.startsWith('pkg:npm/')) {
    name = name.slice('pkg:npm/'.length)
  }

  name = name.replace(/%40/gi, '@')

  // Strip a trailing @version — never a scope separator, which is always followed by `/`.
  name = name.replace(/@[^/@]+$/, '')

  return name
}
