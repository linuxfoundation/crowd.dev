import { canonicalizeGithubRepoUrl } from '@crowd/common'

export interface IDiscussionRefs {
  repoUrls: string[]
  fromTitle: number
  fromBody: number
}

const FENCED_CODE_BLOCK_RE = /(```|~~~)[\s\S]*?\1/g
const INLINE_CODE_RE = /`[^`\n]*`/g
const HTML_COMMENT_RE = /<!--[\s\S]*?-->/g
const BLOCKQUOTE_LINE_RE = /^[ \t]*>.*$/gm

// A link quoted in a code block, HTML comment, or blockquote is context, not a request.
export function stripNonProseSections(markdown: string): string {
  let stripped = markdown
  let previous: string
  do {
    previous = stripped
    stripped = stripped.replace(HTML_COMMENT_RE, '')
  } while (stripped !== previous)

  return stripped
    .replace(FENCED_CODE_BLOCK_RE, '')
    .replace(INLINE_CODE_RE, '')
    .replace(BLOCKQUOTE_LINE_RE, '')
}

const TRAILING_PUNCTUATION_RE = /[.,;:!?)>'"\]]+$/

function stripTrailingPunctuation(text: string): string {
  return text.replace(TRAILING_PUNCTUATION_RE, '')
}

// Excludes gist.github.com and other lookalike hosts (e.g. mygithub.com), which
// would otherwise be silently rewritten into a fabricated github.com/owner/repo URL.
const REPO_REF_RE =
  /(?<![a-zA-Z0-9.-])(?:https?:\/\/)?(?:www\.)?github\.com\/([a-zA-Z0-9_.-]+)\/([a-zA-Z0-9_.-]+)((?:\/[a-zA-Z0-9_.\-/]*)?)/gi

export function extractRepoUrls(text: string): string[] {
  const urls: string[] = []
  const seen = new Set<string>()

  let match: RegExpExecArray | null
  while ((match = REPO_REF_RE.exec(text)) !== null) {
    const owner = match[1]
    const repo = stripTrailingPunctuation(match[2])
    if (!repo) continue

    const rest = stripTrailingPunctuation(match[3] ?? '')
    const canonical = canonicalizeGithubRepoUrl(`https://github.com/${owner}/${repo}${rest}`)
    if (!canonical || seen.has(canonical)) continue

    seen.add(canonical)
    urls.push(canonical)
  }

  return urls
}

export function extractDiscussionRepoUrls(discussion: {
  title: string
  body: string
}): IDiscussionRefs {
  const fromTitleUrls = extractRepoUrls(discussion.title)
  const fromBodyUrls = extractRepoUrls(stripNonProseSections(discussion.body))

  const seen = new Set<string>()
  const repoUrls: string[] = []
  for (const url of [...fromTitleUrls, ...fromBodyUrls]) {
    if (seen.has(url)) continue
    seen.add(url)
    repoUrls.push(url)
  }

  return {
    repoUrls,
    fromTitle: fromTitleUrls.length,
    fromBody: fromBodyUrls.length,
  }
}
