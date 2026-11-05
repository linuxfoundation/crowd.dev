import { GithubPublicClientError, getErrorMessage, mapWithConcurrency } from '@crowd/common'

export type RepositoryLookup = (repoUrl: string) => Promise<boolean>

export interface IUncheckedRepository {
  repoUrl: string
  reason: string
}

export interface IRepositoryCheck {
  missing: string[]
  unchecked: IUncheckedRepository[]
}

type RepositoryOutcome =
  | { repoUrl: string; state: 'present' | 'missing' }
  | { repoUrl: string; state: 'unchecked'; reason: string }

const GITHUB_LOOKUP_CONCURRENCY = 5

async function checkRepository(
  repoUrl: string,
  lookup: RepositoryLookup,
): Promise<RepositoryOutcome> {
  try {
    return { repoUrl, state: (await lookup(repoUrl)) ? 'present' : 'missing' }
  } catch (err) {
    if (err instanceof GithubPublicClientError && err.kind === 'NOT_FOUND') {
      return { repoUrl, state: 'missing' }
    }
    return { repoUrl, state: 'unchecked', reason: getErrorMessage(err) }
  }
}

export async function checkRepositoriesOnGithub(
  repoUrls: string[],
  lookup: RepositoryLookup,
): Promise<IRepositoryCheck> {
  const outcomes = await mapWithConcurrency(repoUrls, GITHUB_LOOKUP_CONCURRENCY, (repoUrl) =>
    checkRepository(repoUrl, lookup),
  )
  return {
    missing: outcomes
      .filter((outcome) => outcome.state === 'missing')
      .map((outcome) => outcome.repoUrl),
    unchecked: outcomes.flatMap((outcome) =>
      outcome.state === 'unchecked' ? [{ repoUrl: outcome.repoUrl, reason: outcome.reason }] : [],
    ),
  }
}
