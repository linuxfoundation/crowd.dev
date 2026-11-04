export const EVENT_RULES_VERSION = 2

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

const PROPOSED = 'proposed 2026-11-04 from the production held queue, awaiting reviewer approval'

const rule = (
  id: string,
  match: { activityUrl: string } | { nameKeyword: string },
  projectSlug: string,
  note = REVIEW,
  approvedBy = 'joana',
): CredlyEventRule => ({ id, projectSlug, approvedBy, note, ...match })

const proposed = (id: string, nameKeyword: string, projectSlug: string): CredlyEventRule =>
  rule(id, { nameKeyword }, projectSlug, PROPOSED, 'pending')

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
  proposed('cloudnativesecuritycon', 'CloudNativeSecurityCon', 'cncf'),
  proposed('cncf', 'CNCF', 'cncf'),
  proposed('cloud-native', 'Cloud Native', 'cncf'),
  proposed('kubernetes-community-days', 'Kubernetes Community Days', 'cncf'),
  proposed('kcd', 'KCD', 'cncf'),
  proposed('kubeday', 'KubeDay', 'cncf'),
  proposed('argocon', 'ArgoCon', 'cncf'),
  proposed('wasmcon', 'WasmCon', 'cncf'),
  proposed('bazelcon', 'BazelCon', 'cncf'),
  proposed('grpconf', 'gRPConf', 'cncf'),
  proposed('observability-day', 'Observability Day', 'cncf'),
  proposed('observability-summit', 'Observability Summit', 'cncf'),
  proposed('otel-community-day', 'OTel Community Day', 'cncf'),
  proposed('platform-engineering-day', 'Platform Engineering Day', 'cncf'),
  proposed('maintainer-summit', 'Maintainer Summit', 'cncf'),
  proposed('istio', 'Istio', 'cncf'),
  proposed('cilium', 'Cilium', 'cncf'),
  proposed('kyverno', 'Kyverno', 'cncf'),
  proposed('backstage', 'Backstage', 'cncf'),
  proposed('prometheus', 'Prometheus', 'cncf'),
  proposed('pytorch', 'PyTorch', 'ptproject'),
  proposed('focus', 'FOCUS', 'finops'),
  proposed('technology-value', 'Technology Value', 'finops'),
  proposed('ai-value', 'AI Value', 'finops'),
  proposed('tokenomics', 'Tokenomics', 'finops'),
  proposed('finos', 'FINOS', 'finos'),
  proposed('fdc3', 'FDC3', 'finos'),
  proposed('open-source-strategy-forum', 'Open Source Strategy Forum', 'finos'),
  proposed('node-red', 'Node-RED', 'node-red'),
  proposed('openjs', 'OpenJS', 'ojsf'),
  proposed('javascriptlandia', 'JavaScriptLandia', 'ojsf'),
  proposed('jsconf', 'JSConf', 'ojsf'),
  proposed('nodejs', 'Node.js', 'ojsf'),
  proposed('onap', 'ONAP', 'onap'),
  proposed('opendaylight', 'OpenDaylight', 'opendaylight'),
  proposed('anuket', 'Anuket', 'anuket'),
  proposed('akraino', 'Akraino', 'akraino'),
  proposed('tungsten-fabric', 'Tungsten Fabric', 'lfn'),
  proposed('fdio', 'FD.io', 'lfn'),
  proposed('one-summit', 'ONE Summit', 'lfn'),
  proposed('open-networking', 'Open Networking', 'lfn'),
  proposed('5g-super-blueprint', '5G Super Blueprint', 'lfn'),
  proposed('lf-networking', 'LF Networking', 'lfn'),
  proposed('magma', 'Magma', 'lfn'),
  proposed('nephio', 'Nephio', 'lfn'),
  proposed('zephyr', 'Zephyr', 'zep'),
  proposed('agntcon', 'AGNTCon', 'agentic-ai-foundation'),
  proposed('mcpcon', 'MCPCon', 'agentic-ai-foundation'),
  proposed('mcp-dev-summit', 'MCP Dev Summit', 'agentic-ai-foundation'),
  proposed('agentic-ai-foundation', 'Agentic AI Foundation', 'agentic-ai-foundation'),
  proposed('kata-containers', 'Kata Containers', 'kata-containers'),
  proposed('openstack', 'OpenStack', 'openinfra-foundation'),
  proposed('openinfra', 'OpenInfra', 'openinfra-foundation'),
  proposed('risc-v', 'RISC-V', 'risc-v-international'),
  proposed('opensearch', 'OpenSearch', 'opensearch-foundation'),
  proposed('opensearchcon', 'OpenSearchCon', 'opensearch-foundation'),
  proposed('cdcon', 'cdCon', 'cdf'),
  proposed('gitopscon', 'GitOpsCon', 'cdf'),
  proposed('cd-foundation', 'CD Foundation', 'cdf'),
  proposed('spinnaker', 'Spinnaker', 'cdf'),
  proposed('jenkins', 'Jenkins', 'cdf'),
  proposed('tekton', 'Tekton', 'cdf'),
  proposed('mainframe', 'Mainframe', 'open-mainframe-project'),
  proposed('zowe', 'Zowe', 'open-mainframe-project'),
  proposed('openssf', 'OpenSSF', 'openssf'),
  proposed('soss', 'SOSS', 'openssf'),
  proposed('hyperledger', 'Hyperledger', 'lf-decentralized-trust'),
  proposed('lfdt', 'LFDT', 'lf-decentralized-trust'),
  proposed('user-conference', 'useR!', 'rcons'),
  proposed('r-medicine', 'R/Medicine', 'rcons'),
  proposed('hpsf', 'HPSF', 'hpsf'),
  proposed('jupyter', 'Jupyter', 'jupyter'),
  proposed('jupytercon', 'JupyterCon', 'jupyter'),
  proposed('academy-software-foundation', 'Academy Software Foundation', 'aswf'),
  proposed('aswf', 'ASWF', 'aswf'),
  proposed('open-source-days', 'Open Source Days', 'aswf'),
  proposed('lf-energy', 'LF Energy', 'lfenergy'),
  proposed('confidential-computing', 'Confidential Computing', 'ccc'),
  proposed('dpdk', 'DPDK', 'dpdk'),
  proposed('cloud-foundry', 'Cloud Foundry', 'cloud-foundry'),
  proposed('px4', 'PX4', 'px4'),
  proposed('prestocon', 'PrestoCon', 'presto-project'),
  proposed('sonic', 'SONiC', 'sonic-foundation'),
  proposed('egeria', 'Egeria', 'odpi-egeria'),
  proposed('automotive-linux', 'Automotive Linux', 'agl'),
  proposed('openapi', 'OpenAPI', 'openapi'),
  proposed('linux-foundation-research', 'Linux Foundation Research', 'tlf'),
  proposed('member-summit', 'Member Summit', 'tlf'),
  proposed('embedded-linux-conference', 'Embedded Linux Conference', 'tlf'),
  proposed('elc', 'ELC', 'tlf'),
  proposed('oss', 'OSS', 'tlf'),
  rule(
    'open-source-summit-url',
    { activityUrl: 'events.linuxfoundation.org/open-source-summit' },
    'tlf',
  ),
  rule('open-source-summit', { nameKeyword: 'Open Source Summit' }, 'tlf'),
  rule('ai-dev', { nameKeyword: 'AI_dev' }, 'tlf'),
  rule('ai-dev-dotted', { nameKeyword: 'AI.dev' }, 'tlf'),
]
