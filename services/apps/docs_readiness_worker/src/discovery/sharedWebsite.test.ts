// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { describe, expect, it } from 'vitest'

import { isUmbrellaWebsite, isUnnamedParentPage } from './sharedWebsite'

const sibling = (name: string) => ({ name, slug: name.toLowerCase().replace(/\W+/g, '-') })

const ODL_SIBLINGS = ['ODL Guice', 'ODL Micro', 'OpenDaylight'].map(sibling)

const GRAPHQL_SIBLINGS = [
  'Express-GraphQL',
  'GraphiQL',
  'GraphQL.js',
  'GraphQL over HTTP',
  'GraphQL Specification',
  'GraphQL Working Group',
].map(sibling)

interface ICase {
  label: string
  name: string
  slug: string
  website: string | null
  siblings: { name: string; slug: string }[]
  umbrella: boolean
}

const CASES: ICase[] = [
  {
    label: 'LFN CNTT: a slug prefix (lfn) must not match the lfnetworking label',
    name: 'CNTT',
    slug: 'lfn-cntt',
    website: 'https://lfnetworking.org/',
    siblings: ['ONAP', 'OPNFV'].map(sibling),
    umbrella: true,
  },
  {
    label: 'Hyperledger Besu: a name token in the hyperledger.org label stays an umbrella',
    name: 'Hyperledger Besu',
    slug: 'hyperledger-besu',
    website: 'https://www.hyperledger.org/',
    siblings: ['Hyperledger Fabric', 'Hyperledger Indy'].map(sibling),
    umbrella: true,
  },
  {
    label: 'a slug-only token no longer anchors a shared site',
    name: 'Something Else',
    slug: 'acme-thing',
    website: 'https://acme.example.org/',
    siblings: ['Unrelated One', 'Unrelated Two'].map(sibling),
    umbrella: true,
  },
  {
    label: 'Electron: the twin "Electron framework" shares the website',
    name: 'Electron',
    slug: 'ojsf-electron',
    website: 'https://www.electronjs.org/',
    siblings: [{ name: 'Electron framework', slug: 'electron-electron' }],
    umbrella: false,
  },
  {
    label: 'Electron framework: the twin "Electron" shares the website',
    name: 'Electron framework',
    slug: 'electron-electron',
    website: 'https://electronjs.org',
    siblings: [{ name: 'Electron', slug: 'ojsf-electron' }],
    umbrella: false,
  },
  {
    label: 'ODL Service Abstraction Framework: family sharing the first name word',
    name: 'ODL Service Abstraction Framework (SAF)',
    slug: 'odl-saf',
    website: 'https://www.opendaylight.org/',
    siblings: ODL_SIBLINGS,
    umbrella: false,
  },
  {
    label: 'ODL Guice: another family member',
    name: 'ODL Guice',
    slug: 'odl-guice',
    website: 'https://www.opendaylight.org/',
    siblings: ['ODL Service Abstraction Framework (SAF)', 'ODL Micro', 'OpenDaylight'].map(sibling),
    umbrella: false,
  },
  {
    label: 'OpenDaylight: the site is named after the project',
    name: 'OpenDaylight',
    slug: 'opendaylight',
    website: 'http://www.opendaylight.org/',
    siblings: ['ODL Guice', 'ODL Micro'].map(sibling),
    umbrella: false,
  },
  {
    label: 'GraphQL IDE Monorepo: graphql.org is named after the project',
    name: 'GraphQL IDE Monorepo',
    slug: 'gql-language-service',
    website: 'https://graphql.org/',
    siblings: GRAPHQL_SIBLINGS,
    umbrella: false,
  },
  {
    label: 'Rez: aswf.io is a foundation umbrella',
    name: 'Rez',
    slug: 'rez',
    website: 'https://www.aswf.io/',
    siblings: ['MaterialX', 'OpenEXR'].map(sibling),
    umbrella: true,
  },
  {
    label: 'LF Energy site is an umbrella even for a same-name family',
    name: 'LF Energy CoMPAS',
    slug: 'compas',
    website: 'https://www.lfenergy.org/',
    siblings: ['LF Energy Everest'].map(sibling),
    umbrella: true,
  },
  {
    label: 'a subdomain of a foundation root is still an umbrella',
    name: 'Something',
    slug: 'something',
    website: 'https://landscape.finos.org/',
    siblings: [sibling('Other')],
    umbrella: true,
  },
  {
    label: 'a twin does not rescue a foundation umbrella',
    name: 'Rez',
    slug: 'rez',
    website: 'https://aswf.io',
    siblings: [sibling('Rez')],
    umbrella: true,
  },
  {
    label: 'an unrelated site shared by unrelated, non-family projects is an umbrella',
    name: 'Alpha Widgets',
    slug: 'alpha-widgets',
    website: 'https://collective.example.org/projects',
    siblings: ['Beta Gadgets', 'Gamma Tools'].map(sibling),
    umbrella: true,
  },
  {
    label: 'a generic first word is no family signal',
    name: 'Open Alpha',
    slug: 'open-alpha',
    website: 'https://collective.example.org/projects',
    siblings: [sibling('Open Beta')],
    umbrella: true,
  },
  {
    label: 'a twin plus an unrelated sibling on an unrelated site stays an umbrella',
    name: 'Alpha',
    slug: 'alpha',
    website: 'https://collective.example.org/projects',
    siblings: ['Alpha framework', 'Beta Gadgets'].map(sibling),
    umbrella: true,
  },
  {
    label: 'no siblings means nothing is shared',
    name: 'Rez',
    slug: 'rez',
    website: 'https://www.aswf.io/',
    siblings: [],
    umbrella: false,
  },
  {
    label: 'no website means nothing is shared',
    name: 'Rez',
    slug: 'rez',
    website: null,
    siblings: [sibling('MaterialX')],
    umbrella: false,
  },
]

describe('isUmbrellaWebsite', () => {
  it.each(CASES)('$label', ({ name, slug, website, siblings, umbrella }) => {
    expect(isUmbrellaWebsite({ name, slug, website, siblings })).toBe(umbrella)
  })
})

describe('isUnnamedParentPage', () => {
  it.each([
    ['shared docs tree', 'https://graphql.org/docs', 'GraphQL.js', true, true],
    ['shared docs host', 'https://docs.fd.io', 'Golang Toolset for VPP (GoVPP)', true, true],
    ['foundation home page', 'https://chipsalliance.org/', 'Chisel Workgroup', false, true],
    [
      'foundation home page, www and trailing slash',
      'https://www.cncf.io',
      'CNCF Binary Project',
      false,
      true,
    ],
    [
      'foundation fund is no foundation',
      'https://chipsalliance.org/',
      'CHIPS Alliance Fund',
      false,
      true,
    ],
    [
      'parent on its shared docs host',
      'https://docs.opendaylight.org',
      'OpenDaylight',
      true,
      false,
    ],
    ['fund twin on its project docs host', 'https://docs.dent.dev', 'DENT Fund', true, false],
    [
      'parent on its foundation home page',
      'https://chipsalliance.org/',
      'CHIPS Alliance',
      false,
      false,
    ],
    [
      'foundation named by its bracketed alias',
      'https://www.cncf.io/',
      'Cloud Native Computing Foundation (CNCF)',
      false,
      false,
    ],
    [
      'foundation with a corporate suffix',
      'https://www.r-consortium.org/',
      'R Consortium, Inc.',
      false,
      false,
    ],
    [
      'project page on a foundation site',
      'https://www.lfedge.org/projects/akraino',
      'Akraino',
      false,
      false,
    ],
    ['unshared page of another site', 'https://docs.example.org/', 'Solo', false, false],
  ])('%s', (_label, url, name, shared, unnamed) => {
    expect(isUnnamedParentPage(url, name, shared)).toBe(unnamed)
  })
})
