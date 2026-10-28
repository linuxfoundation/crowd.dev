import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  listActiveCredlyTemplateMappings,
  listStaleCredlyTemplateMappings,
  lockCredlyTemplateMapping,
} from '@crowd/data-access-layer/src/credly'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { createCredlyDeps } from './deps'

vi.mock('@crowd/data-access-layer/src/credly', () => ({
  listActiveCredlyTemplateMappings: vi.fn(),
  listStaleCredlyTemplateMappings: vi.fn(),
  lockCredlyTemplateMapping: vi.fn(),
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
      lockedAt: new Date('2026-10-27T00:00:00Z'),
    },
    {
      badgeTemplateId: 'tpl-ica',
      badgeTemplateName: 'ICA',
      integrationId: 'int-istio',
      segmentId: 'istio',
      lockedAt: null,
    },
  ])
  mockedStale.mockResolvedValue([
    {
      badgeTemplateId: 'tpl-old',
      badgeTemplateName: 'Old',
      integrationId: 'int-gone',
      segmentId: 'gone',
      lockedAt: null,
    },
  ])
})

describe('createCredlyDeps', () => {
  it('maps templates to their project integration and lists the stale ones', async () => {
    const deps = createCredlyDeps({ qx, integrationId: 'anchor' })

    const routing = await deps.loadTemplateRouting()

    expect(routing.targets.get('tpl-cka')).toEqual({
      segmentId: 'k8s',
      integrationId: 'int-k8s',
      locked: true,
    })
    expect(routing.targets.get('tpl-ica')).toEqual({
      segmentId: 'istio',
      integrationId: 'int-istio',
      locked: false,
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

  it('locks a template through the mapping table', async () => {
    vi.mocked(lockCredlyTemplateMapping).mockResolvedValueOnce(true)
    const deps = createCredlyDeps({ qx, integrationId: 'anchor' })

    const target = { segmentId: 'k8s', integrationId: 'int-k8s', locked: false }

    expect(await deps.lockTemplate('tpl-cka', target)).toBe(true)
    expect(lockCredlyTemplateMapping).toHaveBeenCalledWith(qx, 'tpl-cka', target)
  })
})
