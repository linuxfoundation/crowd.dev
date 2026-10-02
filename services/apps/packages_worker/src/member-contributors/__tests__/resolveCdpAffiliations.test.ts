import { beforeEach, describe, expect, it, vi } from 'vitest'

import * as affiliations from '@crowd/data-access-layer/src/affiliations'
import type { IWorkExperienceResolution } from '@crowd/data-access-layer/src/affiliations'
import * as identities from '@crowd/data-access-layer/src/members/identities'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { MemberIdentityType } from '@crowd/types'

import { AkritesMember } from '../registry'
import { resolveCdpAffiliations } from '../resolveCdpAffiliations'

vi.mock('@crowd/data-access-layer/src/members/identities', () => ({
  findVerifiedMembersByIdentities: vi.fn(),
}))
vi.mock('@crowd/data-access-layer/src/affiliations', () => ({
  resolveCurrentAffiliationsByMemberIds: vi.fn(),
}))

const findMembers = vi.mocked(identities.findVerifiedMembersByIdentities)
const findAffiliations = vi.mocked(affiliations.resolveCurrentAffiliationsByMemberIds)

const qx = {} as QueryExecutor

const registry: AkritesMember[] = [
  { name: 'Cisco', cdpOrganizationIds: ['cisco-1', 'cisco-2'] },
  { name: 'Google', cdpOrganizationIds: ['google-1'] },
]

function affiliation(organizationId: string): IWorkExperienceResolution {
  return {
    id: `row-${organizationId}`,
    memberId: 'm1',
    organizationId,
    organizationName: organizationId,
    title: null,
    dateStart: '2020-01-01',
    dateEnd: null,
    createdAt: '2020-01-01T00:00:00.000Z',
    isPrimaryWorkExperience: false,
    memberCount: 1,
    segmentId: null,
  }
}

beforeEach(() => {
  vi.clearAllMocks()
  findMembers.mockResolvedValue([])
  findAffiliations.mockResolvedValue(new Map())
})

describe('resolveCdpAffiliations', () => {
  it('returns empty and issues no queries for empty input', async () => {
    const out = await resolveCdpAffiliations(qx, [], registry)
    expect(out.size).toBe(0)
    expect(findMembers).not.toHaveBeenCalled()
    expect(findAffiliations).not.toHaveBeenCalled()
  })

  it('batches lowercased deduped logins and emails into one identity query', async () => {
    await resolveCdpAffiliations(
      qx,
      [
        { type: 'github-login', value: 'OctoCat' },
        { type: 'github-login', value: 'octocat' },
        { type: 'email', value: 'Dev@Example.com' },
      ],
      registry,
    )
    expect(findMembers).toHaveBeenCalledTimes(1)
    expect(findMembers).toHaveBeenCalledWith(qx, {
      githubLogins: ['octocat'],
      emails: ['dev@example.com'],
    })
    expect(findAffiliations).not.toHaveBeenCalled()
  })

  it('matches a github login whose member is at a registry org', async () => {
    findMembers.mockResolvedValue([
      {
        memberId: 'm1',
        displayName: 'Octo Cat',
        type: MemberIdentityType.USERNAME,
        value: 'octocat',
      },
    ])
    findAffiliations.mockResolvedValue(new Map([['m1', affiliation('cisco-2')]]))

    const out = await resolveCdpAffiliations(
      qx,
      [{ type: 'github-login', value: 'OctoCat' }],
      registry,
    )

    expect(findAffiliations).toHaveBeenCalledWith(
      qx,
      ['m1'],
      new Set(['cisco-1', 'cisco-2', 'google-1']),
    )
    expect(out.get('github-login:octocat')).toEqual({
      memberId: 'm1',
      displayName: 'Octo Cat',
      akritesMember: 'Cisco',
      cdpOrganizationId: 'cisco-2',
      confidence: 'cdp',
      confidenceScore: 1,
    })
  })

  it('matches an email whose member is at a registry org', async () => {
    findMembers.mockResolvedValue([
      {
        memberId: 'm1',
        displayName: null,
        type: MemberIdentityType.EMAIL,
        value: 'dev@google.com',
      },
    ])
    findAffiliations.mockResolvedValue(new Map([['m1', affiliation('google-1')]]))

    const out = await resolveCdpAffiliations(
      qx,
      [{ type: 'email', value: 'dev@google.com' }],
      registry,
    )

    expect(out.get('email:dev@google.com')?.akritesMember).toBe('Google')
  })

  it('omits a member with no current affiliation', async () => {
    findMembers.mockResolvedValue([
      { memberId: 'm1', displayName: null, type: MemberIdentityType.USERNAME, value: 'octocat' },
    ])
    findAffiliations.mockResolvedValue(new Map([['m1', null]]))

    const out = await resolveCdpAffiliations(
      qx,
      [{ type: 'github-login', value: 'octocat' }],
      registry,
    )

    expect(out.size).toBe(0)
  })

  it('omits a member whose current affiliation is not a registry org', async () => {
    findMembers.mockResolvedValue([
      { memberId: 'm1', displayName: null, type: MemberIdentityType.USERNAME, value: 'octocat' },
    ])
    findAffiliations.mockResolvedValue(new Map([['m1', affiliation('acme')]]))

    const out = await resolveCdpAffiliations(
      qx,
      [{ type: 'github-login', value: 'octocat' }],
      registry,
    )

    expect(out.size).toBe(0)
  })

  it('omits an identity the affiliation lookup reports as ended', async () => {
    findMembers.mockResolvedValue([
      { memberId: 'm1', displayName: null, type: MemberIdentityType.USERNAME, value: 'octocat' },
    ])
    findAffiliations.mockResolvedValue(new Map())

    const out = await resolveCdpAffiliations(
      qx,
      [{ type: 'github-login', value: 'octocat' }],
      registry,
    )

    expect(out.size).toBe(0)
  })

  it('keeps the registry-affiliated member when an email is verified on two members', async () => {
    findMembers.mockResolvedValue([
      {
        memberId: 'm-unaffiliated',
        displayName: null,
        type: MemberIdentityType.EMAIL,
        value: 'jane@cisco.com',
      },
      {
        memberId: 'm-cisco',
        displayName: 'Jane',
        type: MemberIdentityType.EMAIL,
        value: 'jane@cisco.com',
      },
    ])
    findAffiliations.mockResolvedValue(
      new Map([
        ['m-unaffiliated', null],
        ['m-cisco', affiliation('cisco-1')],
      ]),
    )

    const out = await resolveCdpAffiliations(
      qx,
      [{ type: 'email', value: 'jane@cisco.com' }],
      registry,
    )

    expect(out.get('email:jane@cisco.com')?.memberId).toBe('m-cisco')
  })

  it('picks the lowest memberId when an email maps to two registry-affiliated members', async () => {
    const rows = [
      {
        memberId: 'm-b',
        displayName: null,
        type: MemberIdentityType.EMAIL,
        value: 'jane@example.com',
      },
      {
        memberId: 'm-a',
        displayName: null,
        type: MemberIdentityType.EMAIL,
        value: 'jane@example.com',
      },
    ]
    findAffiliations.mockResolvedValue(
      new Map([
        ['m-b', affiliation('google-1')],
        ['m-a', affiliation('cisco-1')],
      ]),
    )

    findMembers.mockResolvedValue(rows)
    const first = await resolveCdpAffiliations(
      qx,
      [{ type: 'email', value: 'jane@example.com' }],
      registry,
    )
    findMembers.mockResolvedValue([...rows].reverse())
    const second = await resolveCdpAffiliations(
      qx,
      [{ type: 'email', value: 'jane@example.com' }],
      registry,
    )

    expect(first.get('email:jane@example.com')?.akritesMember).toBe('Cisco')
    expect(second.get('email:jane@example.com')).toEqual(first.get('email:jane@example.com'))
  })

  it('resolves a login and an email of the same member to two entries with one affiliation lookup', async () => {
    findMembers.mockResolvedValue([
      { memberId: 'm1', displayName: 'Octo', type: MemberIdentityType.USERNAME, value: 'octocat' },
      {
        memberId: 'm1',
        displayName: 'Octo',
        type: MemberIdentityType.EMAIL,
        value: 'octo@cisco.com',
      },
    ])
    findAffiliations.mockResolvedValue(new Map([['m1', affiliation('cisco-1')]]))

    const out = await resolveCdpAffiliations(
      qx,
      [
        { type: 'github-login', value: 'octocat' },
        { type: 'email', value: 'octo@cisco.com' },
      ],
      registry,
    )

    expect(findAffiliations).toHaveBeenCalledWith(qx, ['m1'], expect.any(Set))
    expect(out.get('github-login:octocat')?.memberId).toBe('m1')
    expect(out.get('email:octo@cisco.com')?.memberId).toBe('m1')
  })
})
