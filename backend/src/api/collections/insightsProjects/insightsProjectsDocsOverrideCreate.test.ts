import { beforeEach, describe, expect, it, vi } from 'vitest'

import { BadRequestError } from '@crowd/common'

const createInsightsProjectDocOverride = vi.hoisted(() => vi.fn())

vi.mock('@/services/collectionService', () => ({
  CollectionService: class {
    createInsightsProjectDocOverride = createInsightsProjectDocOverride
  },
}))

vi.mock('../../../services/user/permissionChecker', () => ({
  default: class {
    validateHas = vi.fn()
  },
}))

import handler from './insightsProjectsDocsOverrideCreate'

async function call(body: unknown) {
  const success = vi.fn()
  const req = { params: { id: 'project-1' }, body, responseHandler: { success } }
  await handler(req, {})
  return success
}

describe('insightsProjectsDocsOverrideCreate', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    createInsightsProjectDocOverride.mockResolvedValue({ id: 'override-1' })
  })

  describe('with docsUrl', () => {
    it('passes the URL to the service and returns the override', async () => {
      const success = await call({ docsUrl: 'https://docs.example.com' })

      expect(createInsightsProjectDocOverride).toHaveBeenCalledWith(
        'project-1',
        'https://docs.example.com',
      )
      expect(success).toHaveBeenCalledWith(expect.anything(), expect.anything(), {
        id: 'override-1',
      })
    })
  })

  describe('with noDocs', () => {
    it('passes null (not an empty string) to the service', async () => {
      await call({ noDocs: true })

      expect(createInsightsProjectDocOverride).toHaveBeenCalledWith('project-1', null)
    })
  })

  describe('with an invalid body', () => {
    it.each([
      ['both docsUrl and noDocs', { docsUrl: 'https://docs.example.com', noDocs: true }],
      ['neither docsUrl nor noDocs', {}],
      ['no body', undefined],
      ['noDocs: false', { noDocs: false }],
      ['docsUrl with noDocs: false', { docsUrl: 'https://docs.example.com', noDocs: false }],
      ['an empty docsUrl', { docsUrl: '' }],
      ['a non-URL docsUrl', { docsUrl: 'not a url' }],
      ['a null docsUrl', { docsUrl: null }],
      ['noDocs as a string', { noDocs: 'true' }],
    ])('rejects %s with a BadRequestError', async (_name, body) => {
      await expect(call(body)).rejects.toBeInstanceOf(BadRequestError)
      expect(createInsightsProjectDocOverride).not.toHaveBeenCalled()
    })
  })
})
