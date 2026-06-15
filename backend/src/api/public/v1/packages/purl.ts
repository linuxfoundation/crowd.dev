/**
 * Normalize a PURL for lookup against the packages table.
 *
 * The DB stores versionless PURLs with npm scope @ encoded as %40.
 * Clients may send purls with versions (pkg:npm/lodash@4.17.21) and qualifiers.
 *
 * Transform order:
 *   1. strip ?qualifiers and #subpath — not stored in DB
 *   2. strip @version suffix — DB stores versionless PURLs
 *   3. encode @ in namespace/scope (e.g. npm @babel → %40babel)
 *
 * The version regex matches @ followed by non-/ non-@ chars at end of string.
 * This is always the version separator, not an npm scope (pkg:npm/@babel/core
 * has @babel followed by /core, so it never matches the end-of-string pattern).
 */
export function normalizePurl(purl: string): string {
  const withoutQualifiers = purl.replace(/[?#].*$/, '')
  const withoutVersion = withoutQualifiers.replace(/@[^/@]+$/, '')
  return withoutVersion.replace(/@/g, '%40')
}
