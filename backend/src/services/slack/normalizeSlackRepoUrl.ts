const SLACK_LINK_PATTERN = /^<([^|>]+)(?:\|[^>]*)?>$/
const FORMATTING_CHARS = '`*_~'

function unwrapMatchingDelimiters(value: string): string {
  const first = value[0]
  const isWrapped =
    value.length > 1 && FORMATTING_CHARS.includes(first) && value[value.length - 1] === first
  return isWrapped ? unwrapMatchingDelimiters(value.slice(1, -1)) : value
}

export function normalizeSlackRepoUrl(raw: string): string {
  const unwrapped = unwrapMatchingDelimiters(raw.trim())
  const link = SLACK_LINK_PATTERN.exec(unwrapped)
  return link ? link[1] : unwrapped
}
