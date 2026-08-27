import { isValidEmail } from './email'
import { isPartialEmail } from './validations'

/**
 * Strip email tokens from a name. If the whole string is an email, use the local part.
 */
export function normalizeDisplayName(name: string): string {
  const nameParts = name.trim().split(/\s+/)

  if (nameParts.length === 1 && !nameParts[0]) {
    throw new Error('Display name cannot be empty')
  }

  if (nameParts.length === 0) {
    throw new Error('Display name cannot be empty')
  }

  if (nameParts.length === 1) {
    if (isValidEmail(nameParts[0]) || isPartialEmail(nameParts[0])) {
      return nameParts[0].split('@')[0]
    }
    return nameParts[0]
  }

  const filteredNameParts = nameParts.filter((part) => !isValidEmail(part) && !isPartialEmail(part))

  if (filteredNameParts.length > 0) {
    return filteredNameParts.join(' ')
  }

  return nameParts[0].split('@')[0]
}
