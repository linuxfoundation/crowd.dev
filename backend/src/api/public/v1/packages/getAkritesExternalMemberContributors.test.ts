import type { Request, Response } from 'express'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { BadRequestError, NotFoundError } from '@crowd/common'
import type {
  AffiliatedMemberIdentityRow,
  MemberContributorsPackageRow,
} from '@crowd/data-access-layer'
import { canonicalCdpOrganizationIdByMemberName } from '@crowd/data-access-layer/src/osspckgs/akritesMembers'

import { getAkritesExternalMemberContributors } from './getAkritesExternalMemberContributors'

const { findMemberContributorsPackage, findAffiliatedMemberIdentities } = vi.hoisted(() => ({
  findMemberContributorsPackage: vi.fn(),
  findAffiliatedMemberIdentities: vi.fn(),
}))

vi.mock('@/db/packagesDb', () => ({
  getPackagesQx: vi.fn().mockResolvedValue({}),
}))

vi.mock('@crowd/data-access-layer', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/data-access-layer')>()),
  findMemberContributorsPackage,
  findAffiliatedMemberIdentities,
}))

function packageRow(
  overrides: Partial<MemberContributorsPackageRow> = {},
): MemberContributorsPackageRow {
  return {
    packageId: 'package-1',
    purl: 'pkg:npm/lodash',
    name: 'lodash',
    ecosystem: 'npm',
    repoId: 'repo-1',
    repoUrl: 'https://github.com/lodash/lodash',
    repoMappingConfidence: '0.9',
    hasContributorRows: true,
    ...overrides,
  }
}

function identityRow(
  overrides: Partial<AffiliatedMemberIdentityRow> = {},
): AffiliatedMemberIdentityRow {
  return {
    person: 'person-1',
    source: 'git_activity',
    role: null,
    roleKind: 'contributor',
    identityType: 'github-login',
    identityValue: 'jdalton',
    endedAt: null,
    lastSeenAt: '2026-10-01T00:00:00.000Z',
    registryUsername: null,
    registryDisplayName: null,
    akritesMember: 'Google',
    cdpOrganizationId: 'org-google',
    confidence: 'cdp',
    confidenceScore: '1.0',
    ...overrides,
  }
}

function mockReqRes(query: Record<string, unknown>) {
  const req = { query } as unknown as Request
  const json = vi.fn()
  const status = vi.fn().mockReturnValue({ json })
  const res = { status, json } as unknown as Response
  return { req, res, status, json }
}

const googleOrganization = {
  id: canonicalCdpOrganizationIdByMemberName().get('Google'),
  name: 'Google',
  confidence: 'cdp',
  confidenceScore: 1,
}

describe('getAkritesExternalMemberContributors', () => {
  beforeEach(() => {
    findMemberContributorsPackage.mockReset()
    findAffiliatedMemberIdentities.mockReset()
  })

  it('rejects a malformed purl with a 400 and never queries the database', async () => {
    const { req, res } = mockReqRes({ purl: 'npm/lodash' })

    const error = await getAkritesExternalMemberContributors(req, res).catch((e: unknown) => e)

    expect(error).toBeInstanceOf(BadRequestError)
    expect((error as BadRequestError).status).toBe(400)
    expect((error as BadRequestError).message).toContain('purl must start with pkg:')
    expect(findMemberContributorsPackage).not.toHaveBeenCalled()
    expect(findAffiliatedMemberIdentities).not.toHaveBeenCalled()
  })

  it('rejects an unknown purl with NotFoundError and never reads identities', async () => {
    findMemberContributorsPackage.mockResolvedValue(null)
    const { req, res } = mockReqRes({ purl: 'pkg:npm/unknown' })

    await expect(getAkritesExternalMemberContributors(req, res)).rejects.toBeInstanceOf(
      NotFoundError,
    )
    expect(findAffiliatedMemberIdentities).not.toHaveBeenCalled()
  })

  it('rejects a package without a linked repository and never reads identities', async () => {
    findMemberContributorsPackage.mockResolvedValue(
      packageRow({ repoId: null, repoUrl: null, repoMappingConfidence: null }),
    )
    const { req, res } = mockReqRes({ purl: 'pkg:npm/lodash' })

    const error = await getAkritesExternalMemberContributors(req, res).catch((e: unknown) => e)

    expect(error).toBeInstanceOf(NotFoundError)
    expect((error as NotFoundError).message).toBe('Purl has no linked repository')
    expect(findAffiliatedMemberIdentities).not.toHaveBeenCalled()
  })

  it('responds with the grouped sections when contributors are collected', async () => {
    findMemberContributorsPackage.mockResolvedValue(packageRow())
    findAffiliatedMemberIdentities.mockResolvedValue([
      identityRow(),
      identityRow({
        source: 'registry',
        role: 'maintainer',
        roleKind: 'maintainer',
        registryUsername: 'jdalton',
      }),
      identityRow({
        source: 'governance_file',
        role: 'Lead Maintainer',
        roleKind: 'maintainer',
      }),
    ])
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/lodash' })

    await getAkritesExternalMemberContributors(req, res)

    expect(findAffiliatedMemberIdentities).toHaveBeenCalledWith(expect.anything(), {
      packageId: 'package-1',
      repoId: 'repo-1',
    })
    expect(status).toHaveBeenCalledWith(200)
    const entry = {
      githubLogin: 'jdalton',
      displayName: null,
      emails: [],
      registryUsernames: ['jdalton'],
      sources: ['git_activity', 'governance_file', 'registry'],
      organization: googleOrganization,
    }
    expect(json).toHaveBeenCalledWith({
      purl: 'pkg:npm/lodash',
      name: 'lodash',
      ecosystem: 'npm',
      repoUrl: 'https://github.com/lodash/lodash',
      repoMappingConfidence: 0.9,
      status: 'collected',
      contributors: [entry],
      maintainers: {
        registry: [
          { ...entry, roles: [{ role: 'maintainer', roleKind: 'maintainer', current: true }] },
        ],
        governanceFiles: [
          {
            ...entry,
            roles: [{ role: 'Lead Maintainer', roleKind: 'maintainer', current: true }],
          },
        ],
      },
    })
  })

  it('responds not_collected with only the registry maintainers when the repo has no contributor rows', async () => {
    findMemberContributorsPackage.mockResolvedValue(packageRow({ hasContributorRows: false }))
    findAffiliatedMemberIdentities.mockResolvedValue([
      identityRow({
        source: 'registry',
        role: 'owner',
        roleKind: 'maintainer',
        registryUsername: 'jdalton',
      }),
    ])
    const { req, res, status, json } = mockReqRes({ purl: 'pkg:npm/lodash' })

    await getAkritesExternalMemberContributors(req, res)

    expect(status).toHaveBeenCalledWith(200)
    expect(json).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'not_collected',
        contributors: [],
        maintainers: {
          registry: [
            {
              githubLogin: 'jdalton',
              displayName: null,
              emails: [],
              registryUsernames: ['jdalton'],
              sources: ['registry'],
              organization: googleOrganization,
              roles: [{ role: 'owner', roleKind: 'maintainer', current: true }],
            },
          ],
          governanceFiles: [],
        },
      }),
    )
  })

  it('normalizes the purl before looking the package up', async () => {
    findMemberContributorsPackage.mockResolvedValue(null)
    const { req, res } = mockReqRes({ purl: 'pkg:npm/@scope/x@1.0.0' })

    await expect(getAkritesExternalMemberContributors(req, res)).rejects.toBeInstanceOf(
      NotFoundError,
    )
    expect(findMemberContributorsPackage).toHaveBeenCalledWith(
      expect.anything(),
      'pkg:npm/%40scope/x',
    )
  })
})
