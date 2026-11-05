import { describe, expect, it } from 'vitest'

import { IGithubInsights } from '@/types/githubTypes'

import { mergeGithubInsightsIntoProject } from './githubInsightsMerge'

const insights: IGithubInsights = {
  description: 'An example application demonstrating Apache Kafka with LoopBack 4.',
  github: 'https://github.com/loopbackio',
  logoUrl: 'https://avatars.githubusercontent.com/u/22173486?v=4',
  name: 'loopbackio',
  topics: ['kafka', 'nodejs'],
  twitter: 'StrongLoop',
  website: 'https://loopback.io',
}

describe('mergeGithubInsightsIntoProject', () => {
  it('replaces every field of a non-LF project', () => {
    const current = {
      isLF: false,
      description: 'Old description',
      logoUrl: 'old-logo',
      name: 'Old name',
      keywords: ['old'],
    }

    expect(mergeGithubInsightsIntoProject(current, insights)).toEqual({
      description: insights.description,
      github: insights.github,
      keywords: insights.topics,
      logoUrl: insights.logoUrl,
      name: insights.name,
      twitter: insights.twitter,
      website: insights.website,
    })
  })

  it('keeps the curated fields of an LF project and fills only the empty ones', () => {
    const current = {
      isLF: true,
      name: 'Loopback',
      description: 'LoopBack is a popular Node.js framework for API creation.',
      logoUrl: 'https://lf-master-project-logos-prod.s3.us-east-2.amazonaws.com/loopback.svg',
      website: 'https://loopback.io',
      keywords: [] as string[],
      github: '',
      twitter: null,
    }

    expect(mergeGithubInsightsIntoProject(current, insights)).toEqual({
      github: insights.github,
      keywords: insights.topics,
      twitter: insights.twitter,
    })
  })

  it('leaves an empty LF field untouched when github has nothing for it either', () => {
    const current = {
      isLF: true,
      name: 'Loopback',
      description: 'curated',
      logoUrl: 'curated-logo',
      website: 'https://loopback.io',
      keywords: [] as string[],
      github: '',
      twitter: '',
    }
    const sparseInsights: IGithubInsights = {
      ...insights,
      topics: [],
      twitter: null,
      github: undefined,
    }

    expect(mergeGithubInsightsIntoProject(current, sparseInsights)).toEqual({})
  })

  it('changes nothing on an LF project that already has every field', () => {
    const current = {
      isLF: true,
      name: 'Loopback',
      description: 'curated',
      logoUrl: 'curated-logo',
      website: 'https://loopback.io',
      keywords: ['curated'],
      github: 'https://github.com/loopbackio',
      twitter: 'StrongLoop',
    }

    expect(mergeGithubInsightsIntoProject(current, insights)).toEqual({})
  })
})
