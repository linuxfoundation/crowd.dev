// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { describe, expect, it } from 'vitest'

import { isUmbrellaWebsite } from './sharedWebsite'

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
