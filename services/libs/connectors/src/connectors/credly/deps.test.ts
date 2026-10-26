import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  listActiveCredlyTemplateMappings,
  listStaleCredlyTemplateMappings,
} from '@crowd/data-access-layer/src/credly'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { createCredlyDeps } from './deps'

vi.mock('@crowd/data-access-layer/src/credly', () => ({
  listActiveCredlyTemplateMappings: vi.fn(),
  listStaleCredlyTemplateMappings: vi.fn(),
}))

const mockedActive = vi.mocked(listActiveCredlyTemplateMappings)
const mockedStale = vi.mocked(listStaleCredlyTemplateMappings)
const qx = {} as QueryExecutor

beforeEach(() => {
  vi.clearAllMocks()
  mockedActive.mockResolvedValue([
    {
      badgeTemplateId: 'tpl-cka',
      badgeTemplateName: 'CKA',
      integrationId: 'int-k8s',
      segmentId: 'k8s',
    },
    {
      badgeTemplateId: 'tpl-ica',
      badgeTemplateName: 'ICA',
      integrationId: 'int-istio',
      segmentId: 'istio',
    },
  ])
  mockedStale.mockResolvedValue([
    {
      badgeTemplateId: 'tpl-old',
      badgeTemplateName: 'Old',
      integrationId: 'int-gone',
      segmentId: 'gone',
    },
  ])
})

describe('createCredlyDeps', () => {
  it('maps templates to their project integration and lists the stale ones', async () => {
    const deps = createCredlyDeps({ qx, integrationId: 'anchor' })

    const routing = await deps.loadTemplateRouting()

    expect(routing.targets.get('tpl-cka')).toEqual({ segmentId: 'k8s', integrationId: 'int-k8s' })
    expect(routing.targets.get('tpl-ica')).toEqual({
      segmentId: 'istio',
      integrationId: 'int-istio',
    })
    expect(routing.targets.has('tpl-old')).toBe(false)
    expect(routing.staleTemplateIds).toEqual(['tpl-old'])
  })

  it('reads the mapping once per deps instance, even when called concurrently', async () => {
    const deps = createCredlyDeps({ qx, integrationId: 'anchor' })

    const [first, second] = await Promise.all([
      deps.loadTemplateRouting(),
      deps.loadTemplateRouting(),
    ])
    await deps.loadTemplateRouting()

    expect(first).toBe(second)
    expect(mockedActive).toHaveBeenCalledTimes(1)
    expect(mockedStale).toHaveBeenCalledTimes(1)
  })
})
