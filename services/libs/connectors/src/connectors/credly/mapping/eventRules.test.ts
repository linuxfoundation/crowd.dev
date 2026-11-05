import { describe, expect, it } from 'vitest'

import type { CredlyEventRule } from './eventRules'
import {
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

  it.each(['Zero to Merge - Cohort 01', 'Course Developer', 'LFS158: Introduction to Kubernetes'])(
    'leaves %j to the other rules',
    (name) => {
      expect(matchEventRules(PRODUCTION, { name })).toEqual([])
    },
  )
})
