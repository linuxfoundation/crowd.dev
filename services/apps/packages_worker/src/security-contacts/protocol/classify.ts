import { ClassifierVerdict, ParsedMethod, ProtocolMethodType } from './types'

export const PARSER_VERSION = 1

const EMAIL_RE = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g
const URL_RE = /https?:\/\/[^\s)<>\]"']+/g
const HEADING_RE = /^#{1,6}\s+(.*)$/
const KEYWORD_RE =
  /\b(report|security|vulnerabilit(?:y|ies)|disclosure|disclose|contact|advisor(?:y|ies))\b/i
const NEGATIVE_SECTION_RE = /acknowledg|hall of fame|thanks|credits|honou?r|researchers/i
const PVR_RE =
  /private vulnerability reporting|\/security\/advisories\/new|draft(?:ing)? a (?:new )?security advisory|\bGHSA\b|github security advisor/i
const BOUNTY_RE =
  /hackerone\.com|bugcrowd\.com|huntr\.(?:dev|com)|intigriti\.com|yeswehack\.com|hackenproof\.com|tidelift\.com\/security/i
const MAILING_LIST_RE = /mailing list|lists\.[a-z0-9.-]+\.[a-z]{2,}|groups\.google\.com/i
const FORM_URL_RE = /\/(report|security\/report|vulnerability|disclosure|bounty)[a-z/-]*(\?|$|#)/i
const SECURITY_TXT_RE = /security\.txt/i
const PREFERENCE_RE =
  /\bpreferred\b|\bpreferably\b|\bprimary\b|please use|we (?:strongly )?(?:recommend|encourage|prefer)|first choice/i
const CONDITIONAL_RE =
  /if you (?:cannot|can't|are unable|are unsure)|only (?:if|when)|unless\b|is not possible/i
const NEGATION_RE = /do not|don't|never\b|avoid\b/i
const DEFAULT_TEMPLATE_RE =
  /use this section to tell people about which versions|tell them where to go, how often they can expect/i
const GITHUB_PROFILE_RE = /^https?:\/\/(?:www\.)?github\.com\/[^/]+\/?$/i
const ISSUE_TRACKER_RE = /github\.com\/[^/]+\/[^/]+\/issues/i

const POINTER_TEXT_MAX_CHARS = 200

interface Hit {
  method: ParsedMethod
  line: string
}

function cleanUrl(url: string): string {
  return url.replace(/[.,;:]+$/, '')
}

function urlType(url: string): ProtocolMethodType | null {
  if (BOUNTY_RE.test(url)) return 'bounty-platform'
  if (ISSUE_TRACKER_RE.test(url)) return 'web-form'
  if (FORM_URL_RE.test(url)) return 'web-form'
  return null
}

export function classifySecurityPolicy(text: string): ClassifierVerdict {
  const lines = text.split('\n')
  const hits: Hit[] = []
  const linkedUrls: string[] = []
  const seen = new Set<string>()
  let heading = ''

  const add = (type: ProtocolMethodType, endpoint: string, line: string) => {
    const key = `${type}:${endpoint.toLowerCase()}`
    if (seen.has(key)) return
    seen.add(key)
    hits.push({ method: { type, status: 'accepted', endpoint, condition: null }, line })
  }

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
    const headingMatch = HEADING_RE.exec(line.trim())
    if (headingMatch) {
      heading = headingMatch[1]
      continue
    }
    if (NEGATIVE_SECTION_RE.test(heading)) continue

    if (PVR_RE.test(line)) add('github-pvr', 'github-pvr', line)
    if (SECURITY_TXT_RE.test(line)) add('security-txt', 'security.txt', line)

    const window = lines.slice(Math.max(0, i - 2), i + 3).join('\n')
    if (!KEYWORD_RE.test(window)) continue

    for (const email of line.match(EMAIL_RE) ?? []) add('email', email, line)
    for (const raw of line.match(URL_RE) ?? []) {
      const url = cleanUrl(raw)
      if (/\/security\/advisories/i.test(url)) continue
      if (GITHUB_PROFILE_RE.test(url)) continue
      const type = urlType(url)
      if (type) add(type, url, line)
      else if (linkedUrls.length < 3 && !linkedUrls.includes(url)) linkedUrls.push(url)
    }
    if (MAILING_LIST_RE.test(line)) {
      const listUrl = (line.match(URL_RE) ?? []).map(cleanUrl).find((u) => MAILING_LIST_RE.test(u))
      const listEmail = (line.match(EMAIL_RE) ?? [])[0]
      if (listUrl || listEmail) add('mailing-list', listUrl ?? listEmail, line)
    }
  }

  for (const hit of hits) {
    if (NEGATION_RE.test(hit.line) && hit.method.type !== 'github-pvr') {
      hit.method.status = 'prohibited'
    }
  }

  const usable = hits.filter((h) => h.method.status !== 'prohibited')
  const cued = usable.filter((h) => PREFERENCE_RE.test(h.line))
  const hasConditional = CONDITIONAL_RE.test(text)
  const isTemplate = DEFAULT_TEMPLATE_RE.test(text)

  let clean = false
  if (isTemplate) {
    clean = true
  } else if (!hasConditional && usable.length === 1) {
    usable[0].method.status = 'preferred'
    clean = true
  } else if (!hasConditional && usable.length > 1 && cued.length === 1) {
    cued[0].method.status = 'preferred'
    clean = true
  }

  const residual = text.replace(URL_RE, ' ').replace(/\s+/g, ' ').trim()
  const pointerOnly =
    hits.length === 0 && linkedUrls.length > 0 && residual.length <= POINTER_TEXT_MAX_CHARS

  return {
    clean,
    isTemplate,
    pointerOnly,
    methods: hits.map((h) => h.method),
    linkedUrls,
  }
}
