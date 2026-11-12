import { FetchError, ofetch } from 'ofetch'

import { RepositoryStatus } from '../types'

// Gerrit prefixes every JSON response with this line to prevent XSSI.
const XSSI_PREFIX = ")]}'"

// Gerrit has no archived flag: projects are retired by making them read-only (or hidden), which
// rejects every push and review the same way GitHub's archive does.
const RETIRED_STATES = new Set(['READ_ONLY', 'HIDDEN'])

// All Gerrit instances share the worker's two slots, so a stalled instance must not hold one.
const REQUEST_TIMEOUT_MS = 30000

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

export function isRetiredGerritProject(body: string): boolean {
  const json = body.startsWith(XSSI_PREFIX) ? body.slice(XSSI_PREFIX.length) : body
  const project: { state?: string } = JSON.parse(json)

  return RETIRED_STATES.has(project.state ?? '')
}

export async function getGerritRepoStatus(url: string, baseUrl: string): Promise<RepositoryStatus> {
  const project = gerritProjectName(url, baseUrl)
  const base = baseUrl.replace(/\/+$/, '')

  try {
    const body = await ofetch(`${base}/projects/${encodeURIComponent(project)}`, {
      responseType: 'text',
      headers: { Accept: 'application/json' },
      timeout: REQUEST_TIMEOUT_MS,
    })

    return {
      archived: isRetiredGerritProject(body),
      excluded: false,
    }
  } catch (error: unknown) {
    // Only 404 (deleted, or hidden from anonymous users) means gone. A 403 can be a proxy blocking
    // the client, so it is retried rather than excluding every repository of the instance.
    if (error instanceof FetchError && error.status === 404) {
      console.log(`Gerrit project not found (404): ${url} - marking as excluded`)
      return {
        archived: false,
        excluded: true,
      }
    }

    throw error
  }
}
