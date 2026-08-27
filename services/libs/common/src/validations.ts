import Error400 from './errors/deprecated/Error400'

const URL_REGEXP = new RegExp(
  '^(https?:\\/\\/)?' + // validate protocol
    '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.)+[a-z]{2,}|' + // validate domain name
    '((\\d{1,3}\\.){3}\\d{1,3}))' + // validate OR ip (v4) address
    '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' + // validate port and path
    '(\\?[;&a-z\\d%_.~+=-]*)?' + // validate query string
    '(\\#[-a-z\\d_]*)?$',
  'i',
)

const PARTIAL_EMAIL_LOCAL = new Set("abcdefghijklmnopqrstuvwxyz0123456789!#$%&'*+/=?^_`{|}~-")
const PARTIAL_EMAIL_HOST = new Set('abcdefghijklmnopqrstuvwxyz0123456789-')

export const isUrl = (value: string): boolean => {
  return URL_REGEXP.test(value)
}

export const isPartialEmail = (value: string): boolean => {
  const at = value.indexOf('@')
  if (at <= 0 || at !== value.lastIndexOf('@')) {
    return false
  }

  const local = value.slice(0, at)
  let host = value.slice(at + 1)
  if (host.endsWith('.')) {
    host = host.slice(0, -1)
  }
  if (!local || !host) {
    return false
  }

  return everyCharIn(local, PARTIAL_EMAIL_LOCAL) && everyCharIn(host, PARTIAL_EMAIL_HOST)
}

function everyCharIn(value: string, allowed: Set<string>): boolean {
  for (const char of value) {
    if (!allowed.has(char)) {
      return false
    }
  }
  return true
}

/**
 * Validates non-lf slug to ensure it doesn't contain "illegal" prefixes not supported by LFX (#, !, or %)
 * and returns it prefixed with 'nonlf_'
 * @param slug The slug to validate
 * @returns The validated slug prefixed with 'nonlf_', or throws an error if invalid
 */
export const validateNonLfSlug = (slug: string): string => {
  const illegalLfxPrefixes = ['#', '!', '%']
  const nonLfPrefix = 'nonlf_'

  if (illegalLfxPrefixes.some((prefix) => slug.startsWith(prefix))) {
    throw new Error400(
      `Non-LF Slug cannot start with illegal characters (${illegalLfxPrefixes.join(', ')})`,
    )
  }
  if (!slug.startsWith(nonLfPrefix)) slug = `${nonLfPrefix}${slug}`
  return slug
}
