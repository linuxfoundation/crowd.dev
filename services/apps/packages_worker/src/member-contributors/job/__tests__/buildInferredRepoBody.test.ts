import { beforeEach, describe, expect, it, vi } from 'vitest'

import { canonicalCdpOrganizationIdByMemberName } from '@crowd/data-access-layer/src/osspckgs/akritesMembers'
import * as affiliationsDal from '@crowd/data-access-layer/src/osspckgs/identityAffiliations'

import { buildInferredRepoMemberContributors, purlPackageName } from '../buildInferredRepoBody'

const qx = vi.hoisted(() => ({}))
vi.mock('../../../db', () => ({ getPackagesDb: vi.fn().mockResolvedValue(qx) }))
vi.mock('@crowd/data-access-layer/src/osspckgs/identityAffiliations', async (importOriginal) => ({
  ...(await importOriginal<
    typeof import('@crowd/data-access-layer/src/osspckgs/identityAffiliations')
  >()),
  findAffiliationsForIdentityKeys: vi.fn(),
}))

const find = vi.mocked(affiliationsDal.findAffiliationsForIdentityKeys)

beforeEach(() => vi.clearAllMocks())

describe('purlPackageName', () => {
  it.each([
    ['pkg:hackage/aeson', 'aeson'],
    ['pkg:conan/boost', 'boost'],
    ['pkg:hackage/aeson@2.2.3.0?arch=x#sub', 'aeson'],
    ['pkg:npm/%40babel/core', '@babel/core'],
    ['pkg:huggingface/distilbert-base-uncased', 'distilbert-base-uncased'],
  ])('%s → %s', (purl, name) => {
    expect(purlPackageName(purl)).toBe(name)
  })
})

describe('buildInferredRepoMemberContributors', () => {
  const input = {
    purl: 'pkg:hackage/aeson',
    ecosystem: 'hackage',
    repoUrl: 'https://github.com/haskell/aeson',
    logins: ['octo', 'nobody'],
  }
  const emptyBody = {
    purl: 'pkg:hackage/aeson',
    name: 'aeson',
    ecosystem: 'hackage',
    repoUrl: 'https://github.com/haskell/aeson',
    repoUrlSource: 'llm_inferred',
    repoMappingConfidence: null,
    contributors: [],
    maintainers: { registry: [], governanceFiles: [] },
  }

  it('builds the body from affiliated logins with llm provenance', async () => {
    find.mockResolvedValue([
      {
        person: 'cdp-1',
        source: 'github',
        role: null,
        roleKind: 'contributor',
        identityType: 'github-login',
        identityValue: 'octo',
        endedAt: null,
        lastSeenAt: null,
        registryUsername: null,
        registryDisplayName: null,
        akritesMember: 'Google',
        cdpOrganizationId: 'org-google',
        confidence: 'cdp',
        confidenceScore: '1.0',
      },
    ])

    const body = await buildInferredRepoMemberContributors(input)

    expect(find.mock.calls).toEqual([
      [
        qx,
        [
          { identityType: 'github-login', identityValue: 'octo' },
          { identityType: 'github-login', identityValue: 'nobody' },
        ],
      ],
    ])
    expect(body).toEqual({
      ...emptyBody,
      contributors: [
        {
          githubLogin: 'octo',
          displayName: null,
          emails: [],
          registryUsernames: [],
          sources: ['github'],
          organization: {
            id: canonicalCdpOrganizationIdByMemberName().get('Google'),
            name: 'Google',
            confidence: 'cdp',
            confidenceScore: 1,
          },
        },
      ],
    })
  })

  it('returns an empty body without querying affiliations when there are no logins', async () => {
    await expect(buildInferredRepoMemberContributors({ ...input, logins: [] })).resolves.toEqual(
      emptyBody,
    )
    expect(find).not.toHaveBeenCalled()
  })
})
