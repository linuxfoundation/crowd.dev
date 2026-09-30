const SLACK_LINK_PATTERN = /^<([^|>]+)(?:\|[^>]*)?>$/
const FORMATTING_CHARS_PATTERN = /^[`*_~]+|[`*_~]+$/g

export function normalizeSlackRepoUrl(raw: string): string {
  const unformatted = raw.trim().replace(FORMATTING_CHARS_PATTERN, '')
  const link = SLACK_LINK_PATTERN.exec(unformatted)
  return (link ? link[1] : unformatted).replace(FORMATTING_CHARS_PATTERN, '')
}
