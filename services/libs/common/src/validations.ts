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

const EMAIL_REGEXP = new RegExp(
  "[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?",
)

const PARTIAL_EMAIL_REGEXP = new RegExp("^[a-z0-9!#$%&'*+/=?^_`{|}~-]+@[a-z0-9-]+\\.?$")

export const isUrl = (value: string): boolean => {
  return URL_REGEXP.test(value)
}

export const isEmail = (value: string): boolean => {
  return EMAIL_REGEXP.test(value)
}

export const isPartialEmail = (value: string): boolean => {
  return PARTIAL_EMAIL_REGEXP.test(value)
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
