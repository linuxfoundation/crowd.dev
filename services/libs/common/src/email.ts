import validator from 'validator'

export const isValidEmail = (value: string): boolean => {
  return validator.isEmail(value)
}

const GITHUB_NOREPLY_EMAIL_SUFFIX = '@users.noreply.github.com'

/**
 * Extracts username from a GitHub noreply email.
 * @see https://docs.github.com/en/account-and-profile/reference/email-addresses-reference#your-noreply-email-address
 */
export const parseGitHubNoreplyEmail = (email?: string | null): string | null => {
  if (!email) return null

  const lower = email.toLowerCase()
  if (!lower.endsWith(GITHUB_NOREPLY_EMAIL_SUFFIX)) return null

  const local = lower.slice(0, -GITHUB_NOREPLY_EMAIL_SUFFIX.length)
  if (!local) return null

  const plusIndex = local.indexOf('+')
  const username = plusIndex >= 0 ? local.slice(plusIndex + 1) : local

  return username || null
}
