import axios from 'axios'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { listOrgRepos, resolveRepo } from './discover'

vi.mock('axios')

const mockedGet = vi.mocked(axios.get)

function orgRepos(count: number, offset: number) {
  return Array.from({ length: count }, (_, i) => ({
    node_id: `node-${offset + i}`,
    html_url: `https://github.com/acme/repo-${offset + i}`,
    fork: (offset + i) % 2 === 0,
  }))
}

describe('listOrgRepos', () => {
  beforeEach(() => {
    mockedGet.mockReset()
  })

  it('lists org repos across pages and maps node_id/html_url/fork', async () => {
    mockedGet
      .mockResolvedValueOnce({ data: orgRepos(100, 0) })
      .mockResolvedValueOnce({ data: orgRepos(2, 99) })

    const onPage = vi.fn()
    const repos = await listOrgRepos('token', 'acme', onPage)

    expect(repos).toHaveLength(101)
    expect(onPage).toHaveBeenCalledTimes(2)
    expect(mockedGet).toHaveBeenCalledTimes(2)
    expect(
      mockedGet.mock.calls.map(([url, config]) => [url, (config as { params: unknown }).params]),
    ).toEqual([
      ['https://api.github.com/orgs/acme/repos', { per_page: 100, page: 1, sort: 'full_name' }],
      ['https://api.github.com/orgs/acme/repos', { per_page: 100, page: 2, sort: 'full_name' }],
    ])
    expect(repos[0]).toEqual({
      channelId: 'node-0',
      channelName: 'https://github.com/acme/repo-0',
      fork: true,
    })
    expect(repos[100]).toEqual({
      channelId: 'node-100',
      channelName: 'https://github.com/acme/repo-100',
      fork: true,
    })
  })
})

describe('resolveRepo', () => {
  beforeEach(() => {
    mockedGet.mockReset()
  })

  it('resolves a repo with its parent url when forked, maps the torvalds/linux parent to kernel.org, and null otherwise', async () => {
    mockedGet
      .mockResolvedValueOnce({
        data: {
          node_id: 'node-fork',
          html_url: 'https://github.com/acme/fork',
          parent: { html_url: 'https://github.com/upstream/fork' },
        },
      })
      .mockResolvedValueOnce({
        data: { node_id: 'node-plain', html_url: 'https://github.com/acme/plain' },
      })
      .mockResolvedValueOnce({
        data: {
          node_id: 'node-linux',
          html_url: 'https://github.com/acme/linux',
          parent: { html_url: 'https://github.com/torvalds/linux' },
        },
      })

    await expect(resolveRepo('token', 'acme', 'fork')).resolves.toEqual({
      channelId: 'node-fork',
      channelName: 'https://github.com/acme/fork',
      forkedFrom: 'https://github.com/upstream/fork',
    })
    await expect(resolveRepo('token', 'acme', 'plain')).resolves.toEqual({
      channelId: 'node-plain',
      channelName: 'https://github.com/acme/plain',
      forkedFrom: null,
    })
    await expect(resolveRepo('token', 'acme', 'linux')).resolves.toEqual({
      channelId: 'node-linux',
      channelName: 'https://github.com/acme/linux',
      forkedFrom: 'https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux',
    })
  })
})
