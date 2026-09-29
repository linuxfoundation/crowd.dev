// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { describe, expect, it } from 'vitest'

import { isRelevantSerpResult, nameTokens, projectTokens } from './relevance'

describe('projectTokens', () => {
  it('lowercases alphanumeric words from name, slug and repo owner/name', () => {
    expect(
      projectTokens({
        name: 'Cloudstack on z/VM',
        slug: 'cloudstack-zvm',
        repoUrl: 'https://github.com/openmainframeproject/cloudstack-wg',
      }),
    ).toEqual(['cloudstack', 'zvm', 'openmainframeproject'])
  })

  it('drops generic words and tokens shorter than 3 characters', () => {
    expect(
      projectTokens({
        name: 'Foo Project Foundation for the Open Source Framework Docs Documentation',
        slug: 'foo',
      }),
    ).toEqual(['foo'])
  })

  it('keeps a short token only when it is the whole slug', () => {
    expect(projectTokens({ name: 'VM', slug: 'vm' })).toEqual(['vm'])
    expect(projectTokens({ name: 'VM tools', slug: 'vm-tools' })).toEqual(['tools'])
  })

  it('ignores a repo url that is not a GitHub repo', () => {
    expect(
      projectTokens({ name: 'Foo', slug: 'foo', repoUrl: 'https://gitlab.com/acme/other' }),
    ).toEqual(['foo'])
  })
})

describe('nameTokens', () => {
  it('reduces a product and its "framework" twin to the same tokens', () => {
    expect(nameTokens('Electron')).toEqual(nameTokens('Electron framework'))
  })
})

interface IRelevanceCase {
  project: string
  tokens: string[]
  url: string
  relevant: boolean
}

const tokensOf = (name: string, slug: string, repoUrl?: string) =>
  projectTokens({ name, slug, repoUrl })

const rez = tokensOf('Rez', 'rez', 'https://github.com/academysoftwarefoundation/rez')
const grid2op = tokensOf('Grid2Op', 'grid2op', 'https://github.com/grid2op/grid2op')
const warpx = tokensOf('WarpX', 'warpx', 'https://github.com/blast-warpx/warpx')
const spiderpool = tokensOf(
  'Spiderpool',
  'spiderpool',
  'https://github.com/spidernet-io/spiderpool',
)
const zot = tokensOf('zot', 'zot', 'https://github.com/project-zot/zot')
const cntt = tokensOf('CNTT', 'lfn-cntt')
const architect = tokensOf('Architect', 'ojsf-arc')
const coredns = tokensOf('CoreDNS', 'coredns', 'https://github.com/coredns/coredns')
const cloudstack = tokensOf(
  'Cloudstack on z/VM',
  'cloudstack-zvm',
  'https://github.com/openmainframeproject/cloudstack-wg',
)
const callForCode = tokensOf('Call for Code', 'call-for-code')
const cryptoBroker = tokensOf('Open Crypto Broker', 'open-crypto-broker')
const fiveSpot = tokensOf(
  '5 Spot Machine Scheduler',
  'five-spot-machine-scheduler',
  'https://github.com/finos/5-spot',
)
const electron = tokensOf('Electron', 'ojsf-electron')

const CASES: IRelevanceCase[] = [
  // Stored SERP results that were right.
  ...[
    'https://rez.readthedocs.io/en/stable/commands/rez-help.html',
    'https://rez.readthedocs.io/en/3.1.0/environment.html',
  ].map((url) => ({ project: 'Rez', tokens: rez, url, relevant: true })),
  {
    project: 'Grid2Op',
    tokens: grid2op,
    url: 'https://grid2op.readthedocs.io/en/latest/user/reward.html',
    relevant: true,
  },
  {
    project: 'WarpX',
    tokens: warpx,
    url: 'https://warpx.readthedocs.io/en/26.02/usage/workflows.html',
    relevant: true,
  },
  {
    project: 'zot',
    tokens: zot,
    url: 'https://zotregistry.dev/v2.1.20/install-guides/install-guide-k8s/',
    relevant: true,
  },
  {
    project: 'CNTT',
    tokens: cntt,
    url: 'https://cntt.readthedocs.io/en/stable-kali/gov/chapters/chapter01.html',
    relevant: true,
  },
  {
    project: 'Architect',
    tokens: architect,
    url: 'https://arc.codes/docs/en/guides/plugins/set',
    relevant: true,
  },
  {
    project: 'Spiderpool (github.io tenant, first path segment)',
    tokens: spiderpool,
    url: 'https://spidernet-io.github.io/spiderpool/',
    relevant: true,
  },
  // Vendor doc site that only mentions the project deeper in the path: dropped by design.
  {
    project: 'Spiderpool',
    tokens: spiderpool,
    url: 'https://docs.daocloud.io/network/modules/spiderpool/release-notes/release-1.0/v1.0.0/',
    relevant: false,
  },
  // Stored SERP junk.
  ...[
    'https://redis.io/docs/latest/',
    'https://www.jenkins.io/doc/',
    'https://labelstud.io/guide/',
    'https://en.wikipedia.org/wiki/Documentation',
  ].map((url) => ({ project: 'CoreDNS', tokens: coredns, url, relevant: false })),
  {
    project: 'Cloudstack on z/VM',
    tokens: cloudstack,
    url: 'https://nodejs.org/api/vm.html',
    relevant: false,
  },
  ...[
    'https://www.linkedin.com/top-content/writing/writing-code-documentation/source-code-documentation-guidelines/',
    'https://arxiv.org/html/2404.03114v1',
    'https://dev.to/dubbyding/code-documentation-necessity-or-a-waste-of-time-5bjd',
    'https://techiehub.blog/ai-code-documentation-tools/',
  ].map((url) => ({ project: 'Call for Code', tokens: callForCode, url, relevant: false })),
  {
    project: 'Open Crypto Broker',
    tokens: cryptoBroker,
    url: 'https://www.creativezone.ae/how-to-start-a-crypto-business-in-dubai-step-by-step/',
    relevant: false,
  },
  ...[
    'https://docs.buildbot.net/2.0.1/manual/configuration/schedulers.html',
    'https://docs.redhat.com/en/documentation/openshift_container_platform/4.19/html/nodes/controlling-pod-placement-onto-nodes-scheduling',
    'https://www.instagram.com/reel/DV0MCJSkqcL/',
  ].map((url) => ({ project: '5 Spot Machine Scheduler', tokens: fiveSpot, url, relevant: false })),
  ...[
    'https://www.jenkins.io/doc/',
    'https://grafana.com/docs/',
    'https://en.wikipedia.org/wiki/Documentation',
  ].map((url) => ({ project: 'Electron', tokens: electron, url, relevant: false })),
  ...[
    'https://learn.microsoft.com/ar-sa/entra/architecture/',
    'https://umbrex.com/resources/retail-industry-playbooks/designing-the-sop-architecture-and-documentation-system/',
  ].map((url) => ({ project: 'Architect', tokens: architect, url, relevant: false })),
  // Mechanics.
  {
    project: 'shared hosting tenant that does not match',
    tokens: spiderpool,
    url: 'https://other.github.io/unrelated/spiderpool-fork',
    relevant: false,
  },
  {
    project: 'public suffix is never a match',
    tokens: ['dev'],
    url: 'https://docs.example.dev/',
    relevant: false,
  },
  {
    project: 'noise host wins over a matching token',
    tokens: ['coredns'],
    url: 'https://medium.com/coredns/docs',
    relevant: false,
  },
  { project: 'no tokens', tokens: [], url: 'https://docs.proj.dev/', relevant: false },
  { project: 'unparseable url', tokens: ['proj'], url: 'not a url', relevant: false },
]

describe('isRelevantSerpResult', () => {
  it.each(CASES)('$project: $url -> $relevant', ({ tokens, url, relevant }) => {
    expect(isRelevantSerpResult(url, tokens)).toBe(relevant)
  })
})
