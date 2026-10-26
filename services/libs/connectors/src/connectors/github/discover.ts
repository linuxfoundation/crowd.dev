import axios from 'axios'

import type { Channel, Credential } from '../../types'
import { GITHUB_REQUEST_TIMEOUT_MS, mintInstallationToken, resolveInstallationId } from './appToken'

const PER_PAGE = 100
const GITHUB_LINUX_SUFFIX = 'github.com/torvalds/linux'
const KERNEL_ORG_LINUX = 'https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux'

interface InstallationRepo {
  node_id: string
  html_url: string
}

export interface GithubOrgRepo extends Channel {
  fork: boolean
}

export interface ResolvedGithubRepo extends Channel {
  forkedFrom: string | null
}

interface OrgRepo extends InstallationRepo {
  fork: boolean
}

export async function resolveRepoChannel(
  token: string,
  owner: string,
  name: string,
): Promise<Channel> {
  const response = await axios.get(`https://api.github.com/repos/${owner}/${name}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/vnd.github+json',
    },
    timeout: GITHUB_REQUEST_TIMEOUT_MS,
  })
  return { channelId: response.data.node_id, channelName: response.data.html_url }
}

export async function discoverRepos(credential: Credential): Promise<Channel[]> {
  const installationId = await resolveInstallationId(credential)
  const { token } = await mintInstallationToken(credential, installationId)

  const channels: Channel[] = []
  let page = 1
  for (;;) {
    const response = await axios.get('https://api.github.com/installation/repositories', {
      params: { per_page: PER_PAGE, page },
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github+json',
      },
      timeout: GITHUB_REQUEST_TIMEOUT_MS,
    })
    const repos = (response.data?.repositories ?? []) as InstallationRepo[]
    channels.push(...repos.map((r) => ({ channelId: r.node_id, channelName: r.html_url })))
    if (repos.length < PER_PAGE) {
      return channels
    }
    page += 1
  }
}

export async function listOrgRepos(
  token: string,
  org: string,
  onPage?: () => void,
): Promise<GithubOrgRepo[]> {
  const orgRepos: GithubOrgRepo[] = []
  let page = 1
  for (;;) {
    const response = await axios.get(`https://api.github.com/orgs/${org}/repos`, {
      params: { per_page: PER_PAGE, page, sort: 'full_name' },
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github+json',
      },
      timeout: GITHUB_REQUEST_TIMEOUT_MS,
    })
    const repos = (response.data ?? []) as OrgRepo[]
    orgRepos.push(
      ...repos.map((r) => ({ channelId: r.node_id, channelName: r.html_url, fork: r.fork })),
    )
    onPage?.()
    if (repos.length < PER_PAGE) {
      return [...new Map(orgRepos.map((r) => [r.channelId, r])).values()]
    }
    page += 1
  }
}

export function normalizeForkedFrom(url: string | null): string | null {
  if (!url) {
    return null
  }
  return url.endsWith(GITHUB_LINUX_SUFFIX) ? KERNEL_ORG_LINUX : url
}

export async function resolveRepo(
  token: string,
  owner: string,
  name: string,
): Promise<ResolvedGithubRepo> {
  const response = await axios.get(`https://api.github.com/repos/${owner}/${name}`, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/vnd.github+json',
    },
    timeout: GITHUB_REQUEST_TIMEOUT_MS,
  })
  return {
    channelId: response.data.node_id,
    channelName: response.data.html_url,
    forkedFrom: normalizeForkedFrom(response.data.parent?.html_url ?? null),
  }
}
