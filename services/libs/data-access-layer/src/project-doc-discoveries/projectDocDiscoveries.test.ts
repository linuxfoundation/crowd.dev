import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import { createInsightsProject } from '../collections'
import { findProjectDocDiscovery, upsertProjectDocDiscovery } from './projectDocDiscoveries'

const test = withQx(base)

describe('upsertProjectDocDiscovery', () => {
  test('inserts a row and returns it with parsed candidates', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })

    const row = await upsertProjectDocDiscovery(qx, {
      projectId: project.id,
      docsUrl: 'https://kyverno.io/docs/',
      discoveryMethod: 'docs-path',
      confidence: 'medium',
      candidates: [
        {
          url: 'https://kyverno.io/docs/',
          method: 'docs-path',
          confidence: 'medium',
          livenessOk: true,
        },
      ],
    })

    expect(row.projectId).toBe(project.id)
    expect(row.docsUrl).toBe('https://kyverno.io/docs/')
    expect(row.candidates).toHaveLength(1)
    expect(row.candidates[0].livenessOk).toBe(true)
    expect(row.discoveredAt).not.toBeNull()
  })

  test('overwrites the previous discovery for the same project', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'OpenFGA',
      slug: 'openfga',
      isLF: true,
    })

    await upsertProjectDocDiscovery(qx, {
      projectId: project.id,
      docsUrl: null,
      discoveryMethod: null,
      confidence: null,
      candidates: [],
    })
    const updated = await upsertProjectDocDiscovery(qx, {
      projectId: project.id,
      docsUrl: 'https://openfga.dev/docs',
      discoveryMethod: 'llms-txt-probe',
      confidence: 'high',
      candidates: [],
    })

    expect(updated.docsUrl).toBe('https://openfga.dev/docs')
    expect(updated.discoveryMethod).toBe('llms-txt-probe')

    const found = await findProjectDocDiscovery(qx, project.id)
    expect(found?.docsUrl).toBe('https://openfga.dev/docs')
    expect(await qx.select(`SELECT 1 FROM "projectDocDiscoveries"`)).toHaveLength(1)
  })

  test('findProjectDocDiscovery returns null for an unknown project', async ({ qx }) => {
    expect(await findProjectDocDiscovery(qx, '00000000-0000-0000-0000-000000000000')).toBeNull()
  })
})
