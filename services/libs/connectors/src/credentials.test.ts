import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { getCredential } from './credentials'

function qxReturning(integration: { platform: string } | null): QueryExecutor {
  return { selectOneOrNone: async () => integration } as unknown as QueryExecutor
}

describe('getCredential', () => {
  const ORIGINAL_ENV = process.env

  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV }
  })

  afterEach(() => {
    process.env = ORIGINAL_ENV
  })

  it('throws when the integration does not exist', async () => {
    await expect(getCredential(qxReturning(null), 'missing-integration')).rejects.toThrow(
      'integration missing-integration not found',
    )
  })

  it('throws for an unsupported platform', async () => {
    await expect(getCredential(qxReturning({ platform: 'twitter' }), 'id')).rejects.toThrow(
      'unsupported platform twitter',
    )
  })

  it('returns an octolens-token credential from the shared env var', async () => {
    process.env.CROWD_OCTOLENS_API_TOKEN = 'test-token'

    const credential = await getCredential(qxReturning({ platform: 'octolens' }), 'id')

    expect(credential).toEqual({
      platform: 'octolens',
      kind: 'octolens-token',
      data: { token: 'test-token' },
    })
  })

  it('throws when CROWD_OCTOLENS_API_TOKEN is missing', async () => {
    delete process.env.CROWD_OCTOLENS_API_TOKEN

    await expect(getCredential(qxReturning({ platform: 'octolens' }), 'id')).rejects.toThrow(
      'missing CROWD_OCTOLENS_API_TOKEN environment variable',
    )
  })

  it('returns a credly-oauth-client credential from the shared env vars', async () => {
    process.env.CROWD_CREDLY_CLIENT_ID = 'client-id'
    process.env.CROWD_CREDLY_CLIENT_SECRET = 'client-secret'
    process.env.CROWD_CREDLY_ORGANIZATION_ID = 'org-id'

    const credential = await getCredential(qxReturning({ platform: 'credly' }), 'id')

    expect(credential).toEqual({
      platform: 'credly',
      kind: 'credly-oauth-client',
      data: { clientId: 'client-id', clientSecret: 'client-secret', organizationId: 'org-id' },
    })
  })

  it('throws when any credly env var is missing', async () => {
    process.env.CROWD_CREDLY_CLIENT_ID = 'client-id'
    delete process.env.CROWD_CREDLY_CLIENT_SECRET
    process.env.CROWD_CREDLY_ORGANIZATION_ID = 'org-id'

    await expect(getCredential(qxReturning({ platform: 'credly' }), 'id')).rejects.toThrow(
      'missing CROWD_CREDLY_CLIENT_ID, CROWD_CREDLY_CLIENT_SECRET or CROWD_CREDLY_ORGANIZATION_ID',
    )
  })
})
