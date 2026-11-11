import { ofetch } from 'ofetch'

import { RepositoryStatus } from '../types'

// Gerrit prefixes every JSON response with this line to prevent XSSI.
const XSSI_PREFIX = ")]}'"

// Gerrit has no archived flag: projects are retired by making them read-only (or hidden), which
// rejects every push and review the same way GitHub's archive does.
const RETIRED_STATES = new Set(['READ_ONLY', 'HIDDEN'])

/**
 * Returns the Gerrit project name of a repository URL, which is the URL path below the Gerrit
 * base URL, e.g. `https://gerrit.onap.org/r/aai/aai-common` with base `https://gerrit.onap.org/r`
 * is `aai/aai-common`.
 */
export function gerritProjectName(url: string, baseUrl: string): string {
  const base = baseUrl.replace(/\/+$/, '')
  if (!url.startsWith(`${base}/`)) {
    throw new Error(`Repository URL ${url} is not under its Gerrit base URL ${baseUrl}`)
  }

  const project = url.slice(base.length).replace(/^\/+/, '').replace(/\/+$/, '')
  if (!project) {
    throw new Error(`Invalid Gerrit repository URL: ${url}`)
  }

  return project
}

/**
 * Parses a Gerrit ProjectInfo response body and returns whether the project is retired.
 */
export function isRetiredGerritProject(body: string): boolean {
  const json = body.startsWith(XSSI_PREFIX) ? body.slice(XSSI_PREFIX.length) : body
  const project = JSON.parse(json)

  return RETIRED_STATES.has(project.state)
}

export async function getGerritRepoStatus(url: string, baseUrl: string): Promise<RepositoryStatus> {
  const project = gerritProjectName(url, baseUrl)
  const base = baseUrl.replace(/\/+$/, '')

  try {
    const body = await ofetch(`${base}/projects/${encodeURIComponent(project)}`, {
      responseType: 'text',
      headers: { Accept: 'application/json' },
    })

    return {
      archived: isRetiredGerritProject(body),
      excluded: false,
    }
  } catch (error: any) {
    // 404 means the project was deleted, or hidden (hidden projects are not visible anonymously).
    // A 403 is not treated as gone: Gerrit sites sit behind proxies that can block a client, and
    // excluding every repository of an instance on a transient block would be wrong.
    if (error?.status === 404) {
      console.log(`Gerrit project not found (404): ${url} - marking as excluded`)
      return {
        archived: false,
        excluded: true,
      }
    }

    throw error
  }
}
