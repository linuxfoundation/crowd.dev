import { describe, expect, it } from 'vitest'

import type { CredlyEventRule } from './eventRules'
import {
  EVENT_RULES_VERSION,
  compileEventRules,
  credlyEventRules,
  matchEventRules,
  normalizeActivityUrl,
} from './eventRules'

const base = { approvedBy: 'joana', note: 'test' }
const RULES: readonly CredlyEventRule[] = [
  {
    id: 'kubecon-url',
    activityUrl: 'events.linuxfoundation.org/kubecon',
    projectSlug: 'cncf',
    ...base,
  },
  { id: 'kubecon', nameKeyword: 'KubeCon', projectSlug: 'cncf', ...base },
  { id: 'learn-finops', activityUrl: 'learn.finops.org', projectSlug: 'finops', ...base },
  { id: 'ceph', nameKeyword: 'Ceph', projectSlug: 'cephfoundation', ...base },
  { id: 'ai-dev', nameKeyword: 'AI_dev', projectSlug: 'tlf', ...base },
  { id: 'oss', nameKeyword: 'Open Source Summit', projectSlug: 'tlf', ...base },
]

const COMPILED = compileEventRules(RULES)
const PRODUCTION = compileEventRules(credlyEventRules)

const ids = (name: string, activityUrl?: string | null) =>
  matchEventRules(COMPILED, { name, activityUrl }).map((match) => match.rule.id)

describe('normalizeActivityUrl', () => {
  it.each([
    'https://events.linuxfoundation.org/kubecon-cloudnativecon-europe/',
    'http://www.events.linuxfoundation.org/kubecon-cloudnativecon-europe?utm=x',
    'EVENTS.linuxfoundation.org/kubecon-cloudnativecon-europe#top',
  ])('strips scheme, www, query, fragment and trailing slash from %j', (url) => {
    expect(normalizeActivityUrl(url)).toBe(
      'events.linuxfoundation.org/kubecon-cloudnativecon-europe',
    )
  })
})

describe('matchEventRules', () => {
  it('matches a URL rule on the host and path prefix', () => {
    expect(
      ids('Speaker', 'https://events.linuxfoundation.org/kubecon-cloudnativecon-europe/'),
    ).toEqual(['kubecon-url'])
    expect(ids('Speaker', 'https://events.linuxfoundation.org/kubecon/')).toEqual(['kubecon-url'])
  })

  it('does not match a URL rule on a different path or host', () => {
    expect(ids('Speaker', 'https://events.linuxfoundation.org/open-source-summit-europe/')).toEqual(
      [],
    )
    expect(ids('Speaker', 'https://kubecon.example.org/')).toEqual([])
    expect(ids('Speaker', null)).toEqual([])
  })

  it('does not treat a hyphen as a path continuation on a hostname-only rule', () => {
    expect(ids('Speaker', 'https://learn.finops.org-unrelated.example/course')).toEqual([])
    expect(ids('Speaker', 'https://learn.finops.org/course')).toEqual(['learn-finops'])
    expect(ids('Speaker', 'https://learn.finops.org')).toEqual(['learn-finops'])
  })

  it('matches a keyword rule on whole words, ignoring case', () => {
    expect(ids('Speaker: KUBECON + CloudNativeCon Europe 2025')).toEqual(['kubecon'])
    expect(ids('Cephalocon 2025 Speaker')).toEqual([])
    expect(ids('Ceph Days Speaker')).toEqual(['ceph'])
  })

  it('matches a keyword with punctuation at its edges', () => {
    expect(ids('Speaker: OSS + AI_dev + ALS Japan 2025')).toEqual(['ai-dev'])
  })

  it('returns every matching rule in table order', () => {
    expect(
      ids(
        'Speaker: KubeCon + CloudNativeCon + Open Source Summit + AI_dev China 2024',
        'https://events.linuxfoundation.org/kubecon-cloudnativecon-open-source-summit-ai-dev-china/',
      ),
    ).toEqual(['kubecon-url', 'kubecon', 'ai-dev', 'oss'])
  })
})

describe('credlyEventRules', () => {
  it('is at version 2 with the proposed batch', () => {
    expect(EVENT_RULES_VERSION).toBe(2)
  })

  it('has unique ids and a reviewer on every rule', () => {
    const ruleIds = credlyEventRules.map((rule) => rule.id)
    expect(new Set(ruleIds).size).toBe(ruleIds.length)
    expect(credlyEventRules.every((rule) => rule.approvedBy && rule.note)).toBe(true)
  })

  it.each([
    ['Speaker: KubeCon + CloudNativeCon Europe 2026', 'cncf'],
    ['Program Committee Member: KubeCon + CloudNativeCon North America 2021', 'cncf'],
    ['Golden Kubestronaut', 'cncf'],
    ['Speaker: KubeCon + CloudNativeCon + Open Source Summit + AI_dev China 2024', 'cncf'],
    ['Poster Session Presenter: PyTorch Conference North America 2026', 'ptproject'],
    ['Speaker: Open Source in Finance Forum New York 2025', 'finos'],
    ['Keynote Speaker: Open Source Finance Forum 2023 NYC', 'finos'],
    ['Speaker: Cephalocon 2024', 'cephfoundation'],
    ['Speaker: GraphQLConf 2024', 'gql'],
    ['FinOps Certified FOCUS Analyst', 'finops'],
    ['Speaker: Open Source Summit + Embedded Linux Conference Europe 2026', 'tlf'],
    ['Speaker: Embedded Open Source Summit 2023', 'tlf'],
    ['Speaker: AI.dev 2023', 'tlf'],
  ])('sends %j to %s through the first matching rule', (name, projectSlug) => {
    const [first] = matchEventRules(PRODUCTION, { name })
    expect(first?.rule.projectSlug).toBe(projectSlug)
  })

  it.each([
    ['Organizer: 2024 KCD', 'cncf'],
    ['Contributor - KCD São Paulo, Brazil 2024', 'cncf'],
    ['CNCF Ambassador - H1 2024 - 2026', 'cncf'],
    ['Organizer - 2024 Cloud Native Community Group', 'cncf'],
    ['Speaker: CloudNativeSecurityCon North America 2023', 'cncf'],
    ['2026 KCD & OpenInfra Day Vietnam: Speaker', 'cncf'],
    ['ArgoCon NA 2024 Speaker Badge', 'cncf'],
    ['Speaker: Open Observability Summit + OTel Community Day', 'cncf'],
    ['Istio Day NA 2024 Speaker Badge', 'cncf'],
    ['Fundamentals for Kyverno', 'cncf'],
    ['Speaker: KubeDay India 2023', 'cncf'],
    ['Maintainer Summit Europe 2026 Speaker', 'cncf'],
    ['FOCUS™ Maintainer', 'finops'],
    ['Technology Value Trained: Scopes', 'finops'],
    ['AI Tokenomics Fundamentals 2026', 'finops'],
    ['FINOS Ambassador', 'finos'],
    ['FDC3 2.0 Certified Desktop Agent', 'finos'],
    ['Speaker: Open Source Strategy Forum London 2021', 'finos'],
    ['Node-RED Con 2025', 'node-red'],
    ['OpenJS Foundation: Node-RED', 'node-red'],
    ['OpenJS Foundation: JavaScriptLandia', 'ojsf'],
    ['Exam Developer: OpenJS Node.js Services Developer', 'ojsf'],
    ['ONAP 5 Year Contributor', 'onap'],
    ['OpenDaylight Committer', 'opendaylight'],
    ['Anuket Contributor 2022', 'anuket'],
    ['Akraino Community Awards 2021 - Top Akrainoian', 'akraino'],
    ['FD.io 5-Year Community Contributor', 'lfn'],
    ['Speaker: Open Networking & Edge Summit + Kubernetes on Edge Day 2021', 'lfn'],
    ['Program Committee Member: ONE Summit 2022', 'lfn'],
    ['LF Networking Mentor', 'lfn'],
    ['Magma Mentee Badge 2026', 'lfn'],
    ['Zephyr Technical Contributor', 'zep'],
    ['AGNTCon + MCPCon North America 2026 Speaker Badge', 'agentic-ai-foundation'],
    ['Speaker: MCP Dev Summit Mumbai', 'agentic-ai-foundation'],
    ['Contributor: Kata Containers 4.0', 'kata-containers'],
    ['Contributor: OpenStack Hibiscus 2026.2', 'openinfra-foundation'],
    ['RISC-V Project Mentorship (Mentee)', 'risc-v-international'],
    ['Speaker: OpenSearchCon North America 2025', 'opensearch-foundation'],
    ['Speaker: cdCon + GitOpsCon 2023', 'cdf'],
    ['CD Foundation Ambassador 2024', 'cdf'],
    ['Jenkins Most Valuable Contributor 2026 🏆', 'cdf'],
    ['Open Mainframe Project Mentorship Mentee', 'open-mainframe-project'],
    ['Zowe Committer', 'open-mainframe-project'],
    ['Speaker: SOSS Fusion 2024', 'openssf'],
    ['OpenSSF Community Day North America Speaker 2025', 'openssf'],
    ['Speaker: Hyperledger Global Forum Europe 2022', 'lf-decentralized-trust'],
    ['LFDT Japan Chapter Speaker 2025', 'lf-decentralized-trust'],
    ['Speaker: useR! Conference 2024', 'rcons'],
    ['Program Committee: R/Medicine Virtual Conference 2023', 'rcons'],
    ['Speaker: HPSF Conference 2026', 'hpsf'],
    ['Speaker: JupyterCon 2025', 'jupyter'],
    ['Speaker: ASWF Open Source Days 2026', 'aswf'],
    ['Keynote Speaker: LF Energy Summit 2023', 'lfenergy'],
    ['Confidential Computing Summit 2026 Speaker Badge', 'ccc'],
    ['Speaker: DPDK Summit 2023', 'dpdk'],
    ['Speaker: Cloud Foundry Day Europe 2024', 'cloud-foundry'],
    ['Speaker: PX4 Developer Summit 2023', 'px4'],
    ['Speaker: PrestoCon 2023', 'presto-project'],
    ['SONiC 2026 Hackathon Winner Badge', 'sonic-foundation'],
    ['Egeria Maintainer', 'odpi-egeria'],
    ['Speaker: Automotive Linux Summit 2021', 'agl'],
    ['OpenAPI Volunteer 2023', 'openapi'],
    ['Linux Foundation Research Forum Member 2025', 'tlf'],
    ['Speaker: The Linux Foundation Member Summit 2022', 'tlf'],
    ['Speaker: OSS + ELC North America 2026', 'tlf'],
    ['Program Committee: OSS Japan + ALS+ ELC Asia 2026', 'agl'],
    ['Speaker: Embedded Linux Conference Europe 2020', 'tlf'],
  ])('sends %j to %s through a proposed rule', (name, projectSlug) => {
    const [first] = matchEventRules(PRODUCTION, { name })
    expect(first?.rule.projectSlug).toBe(projectSlug)
  })

  it('does not read OSS inside SOSS or OpenSSF', () => {
    const slugs = (name: string) =>
      matchEventRules(PRODUCTION, { name }).map((m) => m.rule.projectSlug)
    expect(slugs('Speaker: SOSS Community Day Europe 2024')).toEqual(['openssf'])
    expect(slugs('Speaker: OpenSSF Day North America 2023')).toEqual(['openssf'])
  })

  it.each([
    'Zero to Merge - Cohort 01',
    'Course Developer',
    'LFS158: Introduction to Kubernetes',
    'First Contribution',
    'Speaker: seL4 Summit 2026',
    'Speaker: Cassandra Summit 2023',
    'DevOps Bootcamp Graduate',
  ])('leaves %j to the other rules', (name) => {
    expect(matchEventRules(PRODUCTION, { name })).toEqual([])
  })
})
