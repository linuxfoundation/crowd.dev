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

// A link inside a fenced/inline code block, an HTML comment, or a blockquote is
// someone else's link (quoted for context), not a request — strip those sections
// before matching so they can't be picked up as real repo references.
export function stripNonProseSections(markdown: string): string {
  return markdown
    .replace(FENCED_CODE_BLOCK_RE, '')
    .replace(INLINE_CODE_RE, '')
    .replace(HTML_COMMENT_RE, '')
    .replace(BLOCKQUOTE_LINE_RE, '')
}

const TRAILING_PUNCTUATION_RE = /[.,;:!?)>'"\]]+$/

function stripTrailingPunctuation(text: string): string {
  return text.replace(TRAILING_PUNCTUATION_RE, '')
}

const SUB_PATH_SEGMENTS = ['tree', 'blob', 'pull', 'issues', 'discussions']

// Collapses a deep link (blob/tree/pull/issues/discussions) down to the repo root,
// and drops the trailing segment entirely when it's one of those sub-paths with
// nothing else after it (e.g. "owner/repo/issues" -> "owner/repo").
function truncateToRepoRoot(owner: string, repo: string, rest: string): string {
  const firstSegment = rest.split('/').filter(Boolean)[0]
  if (firstSegment && SUB_PATH_SEGMENTS.includes(firstSegment.toLowerCase())) {
    return `https://github.com/${owner}/${repo}`
  }
  return `https://github.com/${owner}/${repo}${rest}`
}

// Matches github.com/{owner}/{repo}[/rest], optionally preceded by a scheme
// (http/https), "www.", or wrapped in a markdown link "[label](url)". The scheme
// is optional because some requests reference the repo without one, e.g. a
// discussion title reading "github.com/agentnameservice/ans".
const REPO_REF_RE =
  /(?:https?:\/\/)?(?:www\.)?github\.com\/([a-zA-Z0-9_.-]+)\/([a-zA-Z0-9_.-]+)((?:\/[a-zA-Z0-9_.\-/]*)?)/gi

export function extractRepoUrls(text: string): string[] {
  const urls: string[] = []
  const seen = new Set<string>()

  let match: RegExpExecArray | null
  while ((match = REPO_REF_RE.exec(text)) !== null) {
    const owner = match[1]
    const repo = stripTrailingPunctuation(match[2])
    if (!repo) continue

    const rest = stripTrailingPunctuation(match[3] ?? '')
    const candidate = truncateToRepoRoot(owner, repo, rest)

    const canonical = canonicalizeGithubRepoUrl(candidate)
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
