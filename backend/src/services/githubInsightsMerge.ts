import { isEmpty, pickBy } from 'lodash'

import { IGithubInsights } from '@/types/githubTypes'
import { ICreateInsightsProject } from '@crowd/data-access-layer/src/collections'

type GithubInsightsProjectFields = Pick<
  ICreateInsightsProject,
  'description' | 'github' | 'keywords' | 'logoUrl' | 'name' | 'twitter' | 'website'
>

export type CuratedInsightsProject = Partial<GithubInsightsProjectFields> &
  Pick<ICreateInsightsProject, 'isLF'>

function githubInsightsToProjectFields(insights: IGithubInsights): GithubInsightsProjectFields {
  return {
    description: insights.description,
    github: insights.github,
    keywords: insights.topics,
    logoUrl: insights.logoUrl,
    name: insights.name,
    twitter: insights.twitter,
    website: insights.website,
  }
}

export function mergeGithubInsightsIntoProject(
  current: CuratedInsightsProject,
  insights: IGithubInsights,
): Partial<GithubInsightsProjectFields> {
  const fields = githubInsightsToProjectFields(insights)
  if (!current.isLF) {
    return fields
  }
  return pickBy(
    fields,
    (value, key) => isEmpty(current[key as keyof GithubInsightsProjectFields]) && !isEmpty(value),
  )
}
