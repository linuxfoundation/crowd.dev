import { describe, expect, it } from 'vitest'

import type { ILifecycleChannel } from '@crowd/data-access-layer/src/connectors'

import {
  diffGithubLifecycle,
  githubRepoKey,
  isRenamedRepo,
  reconcileGithubIntegrationWorkflowId,
} from './githubLifecycle'

const channel = (channelName: string, hasDecommissioned = false): ILifecycleChannel => ({
  channelName,
  hasDecommissioned,
  hasEmitting: !hasDecommissioned,
})

describe('githubRepoKey', () => {
  it.each([
    ['https://github.com/Acme/Widget', 'https://github.com/acme/widget'],
    ['https://github.com/acme/widget/', null],
    ['https://github.com/acme/widget.git', null],
    ['https://github.com/acme', null],
    ['https://gitlab.com/acme/widget', null],
    ['https://github.com/acme/widget/tree/main', null],
    ['https://github.com/acme/widget?per_page=1', null],
    ['https://github.com/acme/..', null],
    [' https://github.com/acme/widget', null],
    ['https://github.com/rr-/docstring_parser', 'https://github.com/rr-/docstring_parser'],
  ])('normalises %s', (url, key) => expect(githubRepoKey(url)).toBe(key))
})

describe('diffGithubLifecycle', () => {
  it('covers the six repo states', () => {
    const result = diffGithubLifecycle({
      deleted: false,
      repoUrls: [
        'https://github.com/Acme/New',
        'https://github.com/acme/new',
        'https://github.com/acme/nango-only',
        'https://github.com/acme/shadow',
        'https://github.com/acme/partial',
        'https://github.com/acme/full',
        'https://github.com/acme/elsewhere',
      ],
      mappingKeys: [
        'https://github.com/acme/nango-only',
        'https://github.com/acme/shadow',
        'https://github.com/acme/partial',
        'https://github.com/acme/full',
      ],
      channels: [
        channel('https://github.com/acme/shadow'),
        channel('https://github.com/acme/partial'),
        channel('https://github.com/acme/full'),
        channel('https://github.com/acme/removed'),
        channel('https://github.com/acme/Removed'),
        channel('not-a-url'),
      ],
      ownedElsewhere: ['https://github.com/acme/elsewhere'],
    })
    expect(result).toEqual({
      toCreate: ['https://github.com/acme/new'],
      toDeleteKeys: ['https://github.com/acme/removed'],
      invalidUrls: [],
      ownedElsewhere: ['https://github.com/acme/elsewhere'],
      deletesSuppressed: false,
    })
  })

  it('deletes every channel and creates nothing for a deleted integration', () => {
    const result = diffGithubLifecycle({
      deleted: true,
      repoUrls: ['https://github.com/acme/a', 'not-a-url'],
      mappingKeys: [],
      channels: [channel('https://github.com/acme/a'), channel('https://github.com/acme/B')],
      ownedElsewhere: [],
    })
    expect(result.toCreate).toEqual([])
    expect(result.toDeleteKeys).toEqual(['https://github.com/acme/a', 'https://github.com/acme/b'])
  })

  it('suppresses undesired deletes when a live integration has an invalid repo url, but still creates and clears decommissioned units', () => {
    const result = diffGithubLifecycle({
      deleted: false,
      repoUrls: ['not-a-url', 'https://github.com/acme/new', 'https://github.com/acme/back'],
      mappingKeys: [],
      channels: [
        channel('https://github.com/acme/widget'),
        channel('https://github.com/acme/back', true),
      ],
      ownedElsewhere: [],
    })
    expect(result).toEqual({
      toCreate: ['https://github.com/acme/new'],
      toDeleteKeys: ['https://github.com/acme/back'],
      invalidUrls: ['not-a-url'],
      ownedElsewhere: [],
      deletesSuppressed: true,
    })
  })

  it('deletes decommissioned units even when their repo is desired again, and does not recreate until they are gone', () => {
    const result = diffGithubLifecycle({
      deleted: false,
      repoUrls: ['https://github.com/acme/back'],
      mappingKeys: [],
      channels: [channel('https://github.com/acme/back', true)],
      ownedElsewhere: [],
    })
    expect(result.toCreate).toEqual([])
    expect(result.toDeleteKeys).toEqual(['https://github.com/acme/back'])
  })

  it('treats casing differences between repositories and channels as the same repo', () => {
    const result = diffGithubLifecycle({
      deleted: false,
      repoUrls: ['https://github.com/Acme/Widget'],
      mappingKeys: [],
      channels: [channel('https://github.com/acme/widget')],
      ownedElsewhere: [],
    })
    expect(result.toCreate).toEqual([])
    expect(result.toDeleteKeys).toEqual([])
  })
})

describe('isRenamedRepo', () => {
  it('compares the channel name to the key case-insensitively', () => {
    expect(isRenamedRepo('https://github.com/acme/widget', 'https://github.com/Acme/Widget')).toBe(
      false,
    )
    expect(isRenamedRepo('https://github.com/acme/widget', 'https://github.com/acme/gadget')).toBe(
      true,
    )
  })
})

describe('reconcileGithubIntegrationWorkflowId', () => {
  it('builds the per-integration workflow id', () => {
    expect(reconcileGithubIntegrationWorkflowId('abc')).toBe(
      'connectors-reconcile-github-integration/abc',
    )
  })
})
