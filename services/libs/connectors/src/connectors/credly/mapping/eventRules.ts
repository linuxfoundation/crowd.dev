export const EVENT_RULES_VERSION = 1

interface EventRuleBase {
  id: string
  projectSlug: string
  approvedBy: string
  note: string
}

export type CredlyEventRule =
  | (EventRuleBase & { activityUrl: string })
  | (EventRuleBase & { nameKeyword: string })

export interface EventRuleTemplate {
  name: string
  activityUrl?: string | null
}

export interface EventRuleMatch {
  rule: CredlyEventRule
  matched: string
}

export interface CompiledEventRule {
  rule: CredlyEventRule
  match: (template: EventRuleTemplate) => string | null
}

export function normalizeActivityUrl(url: string): string {
  return url
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .replace(/[?#].*$/, '')
    .replace(/\/+$/, '')
}

function keywordPattern(keyword: string): RegExp {
  const escaped = keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return new RegExp(`(?<![a-z0-9])${escaped}(?![a-z0-9])`, 'i')
}

function urlMatcher(activityUrl: string): CompiledEventRule['match'] {
  const prefix = normalizeActivityUrl(activityUrl)
  // A hyphen continues a path segment (events.../kubecon-eu), not a hostname: learn.finops.org-x
  // is a different host entirely, so hyphen continuation only applies when the prefix has a path.
  const hasPath = prefix.includes('/')
  return (template) => {
    if (!template.activityUrl) return null
    const url = normalizeActivityUrl(template.activityUrl)
    const matches =
      url === prefix || url.startsWith(`${prefix}/`) || (hasPath && url.startsWith(`${prefix}-`))
    return matches ? url : null
  }
}

function keywordMatcher(keyword: string): CompiledEventRule['match'] {
  const pattern = keywordPattern(keyword)
  return (template) => (pattern.test(template.name) ? keyword : null)
}

export function compileEventRules(rules: readonly CredlyEventRule[]): CompiledEventRule[] {
  return rules.map((rule) => ({
    rule,
    match: 'activityUrl' in rule ? urlMatcher(rule.activityUrl) : keywordMatcher(rule.nameKeyword),
  }))
}

export function matchEventRules(
  rules: readonly CompiledEventRule[],
  template: EventRuleTemplate,
): EventRuleMatch[] {
  const matches: EventRuleMatch[] = []
  for (const { rule, match } of rules) {
    const matched = match(template)
    if (matched !== null) matches.push({ rule, matched })
  }
  return matches
}

const REVIEW =
  'mapping review https://claude.ai/artifact/7p4tERK9DVU5xnQeJPxkV9, decided 2026-10-27'

const rule = (
  id: string,
  match: { activityUrl: string } | { nameKeyword: string },
  projectSlug: string,
  note = REVIEW,
): CredlyEventRule => ({ id, projectSlug, approvedBy: 'joana', note, ...match })

// Ordered: the first matching rule wins, so event-specific rules sit above the LF-wide ones
// that co-located events ("KubeCon + Open Source Summit China") would also match.
export const credlyEventRules: readonly CredlyEventRule[] = [
  rule('kubecon-url', { activityUrl: 'events.linuxfoundation.org/kubecon' }, 'cncf'),
  rule('kubecon', { nameKeyword: 'KubeCon' }, 'cncf'),
  rule('cloudnativecon', { nameKeyword: 'CloudNativeCon' }, 'cncf'),
  rule('kubestronaut', { nameKeyword: 'Kubestronaut' }, 'cncf'),
  rule(
    'pytorch-conference-url',
    { activityUrl: 'events.linuxfoundation.org/pytorch-conference' },
    'ptproject',
  ),
  rule('pytorch-conference', { nameKeyword: 'PyTorch Conference' }, 'ptproject'),
  rule(
    'osff-url',
    { activityUrl: 'events.linuxfoundation.org/open-source-finance-forum' },
    'finos',
  ),
  rule('osff', { nameKeyword: 'Open Source Finance Forum' }, 'finos'),
  rule('osff-in-finance', { nameKeyword: 'Open Source in Finance Forum' }, 'finos'),
  rule('cephalocon', { nameKeyword: 'Cephalocon' }, 'cephfoundation'),
  rule('graphqlconf', { nameKeyword: 'GraphQLConf' }, 'gql'),
  rule('graphql', { nameKeyword: 'GraphQL' }, 'gql'),
  rule('learn-finops', { activityUrl: 'learn.finops.org' }, 'finops'),
  rule('finops', { nameKeyword: 'FinOps' }, 'finops'),
  rule(
    'open-source-summit-url',
    { activityUrl: 'events.linuxfoundation.org/open-source-summit' },
    'tlf',
  ),
  rule('open-source-summit', { nameKeyword: 'Open Source Summit' }, 'tlf'),
  rule('ai-dev', { nameKeyword: 'AI_dev' }, 'tlf'),
  rule('ai-dev-dotted', { nameKeyword: 'AI.dev' }, 'tlf'),
]
