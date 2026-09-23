import { ApplicationFailure } from '@temporalio/client'

export function parseGithubRepoUrl(url: string): { owner: string; name: string } {
  let parsed: URL
  try {
    parsed = new URL(url.replace('git@github.com:', 'https://github.com/'))
  } catch {
    throw ApplicationFailure.nonRetryable(`Cannot parse GitHub URL: ${url}`, 'INVALID_URL')
  }

  const pathParts = parsed.pathname
    .replace(/^\//, '')
    .replace(/\/$/, '')
    .replace(/\.git$/, '')
    .split('/')

  if (
    parsed.hostname !== 'github.com' ||
    pathParts.length !== 2 ||
    !pathParts[0] ||
    !pathParts[1]
  ) {
    throw ApplicationFailure.nonRetryable(`Cannot parse GitHub URL: ${url}`, 'INVALID_URL')
  }

  return { owner: pathParts[0], name: pathParts[1] }
}
