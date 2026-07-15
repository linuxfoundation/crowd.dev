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
import { z } from 'zod'

function stripQualifiers(purl: string): string {
  const q = purl.indexOf('?')
  const h = purl.indexOf('#')
  if (q === -1 && h === -1) return purl
  if (q === -1) return purl.slice(0, h)
  if (h === -1) return purl.slice(0, q)
  return purl.slice(0, Math.min(q, h))
}

export function normalizePurl(purl: string): string {
  const withoutQualifiers = stripQualifiers(purl)
  const withoutVersion = withoutQualifiers.replace(/@[^/@]+$/, '')
  return withoutVersion.replace(/@/g, '%40')
}

export const purlFieldSchema = z
  .string()
  .trim()
  .min(1)
  .refine((v) => v.startsWith('pkg:'), { message: 'purl must start with pkg:' })
  .transform(normalizePurl)

export const purlQuerySchema = z.object({ purl: purlFieldSchema })

// Loose schema for search filters: normalizes without requiring the pkg: prefix,
// so partial inputs (e.g. "@babel/core" or "lodash") are accepted.
export const purlFilterSchema = z.string().trim().transform(normalizePurl).optional()

export const MAX_PURLS_PER_BATCH = 100

// Unlike purlFieldSchema, this does NOT normalize — batch endpoints normalize
// after parsing so they can echo back the client's original (un-normalized) purl.
const purlArrayItemSchema = z
  .string()
  .trim()
  .min(1)
  .refine((v) => v.startsWith('pkg:'), { message: 'each purl must start with pkg:' })

export function purlsBodySchema(max: number = MAX_PURLS_PER_BATCH) {
  return z.object({
    purls: z.array(purlArrayItemSchema).min(1).max(max, `Maximum ${max} purls per request`),
  })
}
