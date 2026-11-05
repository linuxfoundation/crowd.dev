import { test as base, describe, expect } from 'vitest'

import type { QueryExecutor } from '@crowd/database'
import { withQx } from '@crowd/test-kit/db'
import {
  createMemberIdentities,
  createMemberOrganizations,
  createMembers,
  createOrganizations,
  withMemberDefaults,
  withMemberIdentityDefaults,
  withOrganizationDefaults,
} from '@crowd/test-kit/factories'
import { MemberIdentityType, PlatformType } from '@crowd/types'

import { updateMemberIdentity, updateVerifiedFlag } from './identities'
import { createMemberOrganization, updateMemberOrganization } from './organizations'

const test = withQx(base)

const EMAIL = 'ada@example.com'

async function createMember(qx: QueryExecutor) {
  const [member] = await createMembers(qx, withMemberDefaults([{ displayName: 'Ada Lovelace' }]))
  return member
}

async function createEmailIdentity(
  qx: QueryExecutor,
  memberId: string,
  verified: boolean,
  stampVerifiedAt?: boolean,
) {
  const [identity] = await createMemberIdentities(
    qx,
    withMemberIdentityDefaults([
      {
        memberId,
        platform: PlatformType.GITHUB,
        value: EMAIL,
        type: MemberIdentityType.EMAIL,
        verified,
        verifiedBy: 'ada-admin',
        stampVerifiedAt,
      },
    ]),
  )
  return identity
}

async function createMemberWithOrganization(qx: QueryExecutor) {
  const member = await createMember(qx)
  const [org] = await createOrganizations(
    qx,
    withOrganizationDefaults([{ displayName: 'Analytical Engines' }]),
  )
  return { member, org }
}

async function fetchIdentity(qx: QueryExecutor, id: string) {
  return qx.selectOne(
    `SELECT verified, "verifiedAt", "updatedAt" FROM "memberIdentities" WHERE id = $(id)`,
    { id },
  )
}

async function fetchMemberOrganization(qx: QueryExecutor, id: string) {
  return qx.selectOne(
    `SELECT verified, "verifiedAt", "updatedAt" FROM "memberOrganizations" WHERE id = $(id)`,
    { id },
  )
}

describe('insertMemberIdentities', () => {
  test('stamps verifiedAt with the insert timestamp when asked', async ({ qx }) => {
    const member = await createMember(qx)

    const identity = await createEmailIdentity(qx, member.id, true, true)

    expect(identity.verifiedAt).not.toBeNull()
    expect(identity.verifiedAt).toBe(identity.updatedAt)
  })

  test('leaves verifiedAt null without the stamp option', async ({ qx }) => {
    const member = await createMember(qx)

    const identity = await createEmailIdentity(qx, member.id, true)

    expect(identity.verifiedAt).toBeNull()
  })
})

describe('updateMemberIdentity', () => {
  test('stamps verifiedAt when verifying with the stamp option', async ({ qx }) => {
    const member = await createMember(qx)
    const identity = await createEmailIdentity(qx, member.id, false)

    const updated = await updateMemberIdentity(
      qx,
      member.id,
      identity.id,
      { verified: true },
      { stampVerifiedAt: true },
    )

    expect(updated.verifiedAt).not.toBeNull()
    expect(updated.verifiedAt).toBe(updated.updatedAt)
  })

  test('clears verifiedAt when unverifying', async ({ qx }) => {
    const member = await createMember(qx)
    const identity = await createEmailIdentity(qx, member.id, true, true)

    const updated = await updateMemberIdentity(qx, member.id, identity.id, { verified: false })

    expect(updated.verified).toBe(false)
    expect(updated.verifiedAt).toBeNull()
  })

  test('ignores a verifiedAt value passed in the update data', async ({ qx }) => {
    const member = await createMember(qx)
    const identity = await createEmailIdentity(qx, member.id, true)

    const updated = await updateMemberIdentity(qx, member.id, identity.id, {
      verifiedBy: 'grace-admin',
      verifiedAt: '2020-01-01T00:00:00.000Z',
    })

    expect(updated.verifiedBy).toBe('grace-admin')
    expect(updated.verifiedAt).toBeNull()
  })
})

describe('updateVerifiedFlag', () => {
  test('keeps the existing verifiedAt when verifying', async ({ qx }) => {
    const member = await createMember(qx)
    const identity = await createEmailIdentity(qx, member.id, false)
    await qx.result(
      `UPDATE "memberIdentities" SET "verifiedAt" = '2024-03-01 10:00:00+00' WHERE id = $(id)`,
      { id: identity.id },
    )

    await updateVerifiedFlag(qx, {
      memberId: member.id,
      platform: PlatformType.GITHUB,
      value: EMAIL,
      type: MemberIdentityType.EMAIL,
      verified: true,
    })

    const row = await fetchIdentity(qx, identity.id)
    expect(row.verified).toBe(true)
    expect(new Date(row.verifiedAt).toISOString()).toBe('2024-03-01T10:00:00.000Z')
  })

  test('clears verifiedAt when unverifying', async ({ qx }) => {
    const member = await createMember(qx)
    const identity = await createEmailIdentity(qx, member.id, true, true)

    await updateVerifiedFlag(qx, {
      memberId: member.id,
      platform: PlatformType.GITHUB,
      value: EMAIL,
      type: MemberIdentityType.EMAIL,
      verified: false,
    })

    const row = await fetchIdentity(qx, identity.id)
    expect(row.verified).toBe(false)
    expect(row.verifiedAt).toBeNull()
  })
})

describe('updateMemberOrganization', () => {
  test('stamps verifiedAt when verifying with the stamp option', async ({ qx }) => {
    const { member, org } = await createMemberWithOrganization(qx)
    const [mo] = await createMemberOrganizations(qx, [
      { memberId: member.id, organizationId: org.id, verified: false },
    ])

    const updated = await updateMemberOrganization(
      qx,
      member.id,
      mo.id,
      { verified: true, verifiedBy: 'ada-admin' },
      { stampVerifiedAt: true },
    )

    expect(updated.verifiedAt).not.toBeNull()
    expect(updated.verifiedAt).toBe(updated.updatedAt)
  })

  test('clears verifiedAt when unverifying', async ({ qx }) => {
    const { member, org } = await createMemberWithOrganization(qx)
    const [mo] = await createMemberOrganizations(qx, [
      { memberId: member.id, organizationId: org.id, verified: true, stampVerifiedAt: true },
    ])

    await updateMemberOrganization(qx, member.id, mo.id, { verified: false })

    const row = await fetchMemberOrganization(qx, mo.id)
    expect(row.verified).toBe(false)
    expect(row.verifiedAt).toBeNull()
  })
})

describe('createMemberOrganization', () => {
  test('stamps verifiedAt with the same timestamp as updatedAt', async ({ qx }) => {
    const { member, org } = await createMemberWithOrganization(qx)

    const id = await createMemberOrganization(
      qx,
      member.id,
      { organizationId: org.id, verified: true, verifiedBy: 'ada-admin' },
      { stampVerifiedAt: true },
    )

    const row = await fetchMemberOrganization(qx, id)
    expect(row.verifiedAt).not.toBeNull()
    expect(row.verifiedAt).toBe(row.updatedAt)
  })
})
