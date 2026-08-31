import { isValidEmail } from './email'
import { isPartialEmail } from './validations'

/**
 * Strip email tokens from a name. If the whole string is an email, use the local part.
 */
export function normalizeDisplayName(name: string): string {
  const tokens = name
    .trim()
    .split(/\s+/)
    .map(cleanNamePart)
    .filter((token) => token.length > 0)

  if (tokens.length === 0) {
    throw new Error('Display name cannot be empty')
  }

  const withoutEmails = tokens.filter((token) => !isValidEmail(token) && !isPartialEmail(token))
  if (withoutEmails.length > 0) {
    return withoutEmails.join(' ')
  }

  return tokens[0].split('@')[0]
}

function cleanNamePart(part: string): string {
  let token = part

  if (token.endsWith(',') || token.endsWith(';')) {
    token = token.slice(0, -1)
  }

  while (token.length >= 2) {
    const open = token[0]
    const close = token[token.length - 1]
    const isWrapped =
      (open === '<' && close === '>') ||
      (open === '(' && close === ')') ||
      (open === '"' && close === '"') ||
      (open === "'" && close === "'")

    if (!isWrapped) {
      break
    }

    token = token.slice(1, -1)
  }

  while (token.startsWith('@')) {
    token = token.slice(1)
  }
  while (token.endsWith('@')) {
    token = token.slice(0, -1)
  }

  return token
}
