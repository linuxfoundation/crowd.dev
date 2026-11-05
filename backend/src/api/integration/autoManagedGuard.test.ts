import { beforeEach, describe, expect, it, vi } from 'vitest'

import { Error400 } from '@crowd/common'

const service = vi.hoisted(() => ({
  create: vi.fn(),
  update: vi.fn(),
  destroyAll: vi.fn(),
}))
const findIntegrationPlatformsByIds = vi.hoisted(() => vi.fn())

vi.mock('../../services/integrationService', () => ({
  default: class {
    create = service.create
    update = service.update
    destroyAll = service.destroyAll
  },
}))

vi.mock('../../services/user/permissionChecker', () => ({
  default: class {
    validateHas = vi.fn()
  },
}))

vi.mock('@/database/repositories/sequelizeRepository', () => ({
  default: { getQueryExecutor: vi.fn(() => ({})) },
}))

vi.mock('@crowd/data-access-layer/src/integrations', () => ({ findIntegrationPlatformsByIds }))

import integrationCreate from './integrationCreate'
import integrationDestroy from './integrationDestroy'
import integrationUpdate from './integrationUpdate'

const GITHUB_ID = '6f1c2a5e-8f4b-4d3a-9b2c-1a2b3c4d5e6f'
const OCTOLENS_ID = '0c9d8e7f-6a5b-4c3d-8e2f-9a8b7c6d5e4f'
const platformsById: Record<string, string> = { [GITHUB_ID]: 'github', [OCTOLENS_ID]: 'octolens' }

const noHyphens = (id: string) => id.replace(/-/g, '')
const braced = (id: string) => `{${id.toUpperCase()}}`

async function call(
  handler: (req: unknown, res: unknown) => Promise<void>,
  req: Record<string, unknown>,
) {
  const success = vi.fn()
  await handler({ language: 'en', responseHandler: { success }, ...req }, {})
  return success
}

describe('auto-managed integration guard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    findIntegrationPlatformsByIds.mockImplementation(async (_qx, ids: string[]) =>
      ids.map((id) => platformsById[id]).filter(Boolean),
    )
  })

  it.each([
    ['another platform', { platform: 'github' }],
    ['no platform', {}],
    ['no body', undefined],
  ])('creates as before for %s', async (_name, body) => {
    const success = await call(integrationCreate, { body })

    expect(service.create).toHaveBeenCalledWith(body)
    expect(success).toHaveBeenCalled()
  })

  it('refuses to create an octolens integration', async () => {
    await expect(call(integrationCreate, { body: { platform: 'octolens' } })).rejects.toThrow(
      /managed automatically/,
    )
    expect(service.create).not.toHaveBeenCalled()
  })

  it.each([
    ['another platform', GITHUB_ID],
    ['another platform without hyphens', noHyphens(GITHUB_ID)],
    ['another platform in braces', braced(GITHUB_ID)],
    ['an unknown id', '11111111-2222-4333-8444-555555555555'],
    ['a malformed id', 'not-a-uuid'],
  ])('updates as before for %s', async (_name, id) => {
    await call(integrationUpdate, { params: { id }, body: { status: 'done' } })

    expect(service.update).toHaveBeenCalledWith(id, { status: 'done' })
  })

  it.each([
    ['canonical', OCTOLENS_ID],
    ['without hyphens', noHyphens(OCTOLENS_ID)],
    ['in braces', braced(OCTOLENS_ID)],
  ])('refuses to update an octolens integration given a %s id', async (_name, id) => {
    await expect(
      call(integrationUpdate, { params: { id }, body: { status: 'done' } }),
    ).rejects.toBeInstanceOf(Error400)
    expect(service.update).not.toHaveBeenCalled()
  })

  it('refuses to change an integration platform to octolens', async () => {
    await expect(
      call(integrationUpdate, { params: { id: GITHUB_ID }, body: { platform: 'octolens' } }),
    ).rejects.toBeInstanceOf(Error400)
    expect(service.update).not.toHaveBeenCalled()
  })

  it.each([
    ['other platforms', [GITHUB_ID]],
    ['other platforms without hyphens or in braces', [noHyphens(GITHUB_ID), braced(GITHUB_ID)]],
    ['malformed ids', ['not-a-uuid']],
    ['no ids', undefined],
    ['other platforms in nested arrays', [[GITHUB_ID]]],
  ])('destroys as before for %s', async (_name, ids) => {
    await call(integrationDestroy, { query: { ids } })

    expect(service.destroyAll).toHaveBeenCalledWith(ids)
  })

  it.each([
    ['one of several ids', [GITHUB_ID, OCTOLENS_ID]],
    ['a single id', OCTOLENS_ID],
    ['an id without hyphens', [noHyphens(OCTOLENS_ID)]],
    ['an id in braces', [braced(OCTOLENS_ID)]],
    ['an id in a nested array', [[OCTOLENS_ID]]],
    ['an id deeply nested in arrays', [GITHUB_ID, [[OCTOLENS_ID]]]],
    ['an id nested in an object', { a: OCTOLENS_ID }],
    ['an id nested in an array of objects', [{ x: [OCTOLENS_ID] }]],
  ])('refuses to destroy when %s is an octolens integration', async (_name, ids) => {
    await expect(call(integrationDestroy, { query: { ids } })).rejects.toBeInstanceOf(Error400)
    expect(service.destroyAll).not.toHaveBeenCalled()
  })
})
