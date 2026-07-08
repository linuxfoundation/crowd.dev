import { parse } from 'tldts'

import { emailProviders } from './constants'

interface ParseResult {
  hostname: string | null
  isIp: boolean | null
  subdomain: string | null
  domain: string | null
  publicSuffix: string | null
  domainWithoutSuffix: string | null
  isIcann: boolean | null
  isPrivate: boolean | null
}

export const cleanURL = (url: string): string =>
  url.replace(/^(?:https?:\/\/)?(?:www\.)?([^/]+)(?:\/.*)?$/i, '$1')

export const isValidDomain = (parsed: ParseResult): boolean =>
  Boolean(parsed.domain && parsed.isIcann)

/**
 * Returns the normalized hostname (subdomain + domain) in lowercase.
 * Throws if the URL is invalid, unless `throwError` is false.
 */
export const normalizeHostname = (url: string, throwError = true): string | undefined => {
  const cleanDomain = cleanURL(url)
  const parsed = parse(cleanDomain)

  if (!isValidDomain(parsed)) {
    if (throwError) {
      throw new Error(`Invalid website URL '${url}' - clean '${cleanDomain}'!`)
    }
    return undefined
  }

  const domain = parsed.domain?.toLowerCase()
  const subdomain = parsed.subdomain ? `${parsed.subdomain.toLowerCase()}.` : ''

  const normalized = `${subdomain}${domain}`

  return normalized
}

/**
 * Checks if a domain is part of the excluded domains.
 * These domains are public email providers and not organizations.
 *
 * @param {string} domain - The domain name to check.
 * @return {boolean} true if the domain is excluded, false otherwise.
 */
export const isDomainExcluded = (domain: string) => {
  return emailProviders.has(domain.toLowerCase())
}

/**
 * Extracts the main domain part from a domain (without TLD and subdomains)
 * Examples:
 * - "amazon.com" → "amazon"
 * - "aws.amazon.com" → "amazon"
 * - "amazon.co.uk" → "amazon"
 * - "aws.amazon.co.uk" → "amazon"
 */
export const getDomainRootLabel = (url: string): string | null => {
  const cleanDomain = cleanURL(url)
  const parsed = parse(cleanDomain)

  if (!isValidDomain(parsed)) {
    return null
  }

  return parsed.domainWithoutSuffix?.toLowerCase() ?? null
}
