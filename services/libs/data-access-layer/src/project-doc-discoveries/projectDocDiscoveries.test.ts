import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import { createInsightsProject } from '../collections'
import {
  findProjectDocDiscovery,
  findSharedDocsUrls,
  upsertProjectDocDiscovery,
} from './projectDocDiscoveries'

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

describe('findSharedDocsUrls', () => {
  test('returns docs URLs used by other projects, excluding the given one and nulls', async ({
    qx,
  }) => {
    const mk = (slug: string) => createInsightsProject(qx, { name: slug, slug, isLF: true })
    const [a, b, c] = await Promise.all([mk('a-proj'), mk('b-proj'), mk('c-proj')])
    const upsert = (projectId: string, docsUrl: string | null) =>
      upsertProjectDocDiscovery(qx, {
        projectId,
        docsUrl,
        discoveryMethod: docsUrl ? 'project-website' : null,
        confidence: docsUrl ? 'low' : null,
        candidates: [],
      })

    await upsert(a.id, 'https://foundation.org')
    await upsert(b.id, 'https://foundation.org')
    await upsert(c.id, null)

    expect(await findSharedDocsUrls(qx, a.id)).toEqual(['https://foundation.org'])
    expect(await findSharedDocsUrls(qx, c.id)).toEqual(['https://foundation.org'])
  })

  test('ignores rows of disabled and soft-deleted projects', async ({ qx }) => {
    const [a, gone, off] = await Promise.all(
      ['a2', 'gone', 'off'].map((slug) =>
        createInsightsProject(qx, { name: slug, slug, isLF: true }),
      ),
    )
    for (const p of [a, gone, off]) {
      await upsertProjectDocDiscovery(qx, {
        projectId: p.id,
        docsUrl: `https://${p.id}.example.org`,
        discoveryMethod: 'project-website',
        confidence: 'low',
        candidates: [],
      })
    }
    await qx.result(`UPDATE "insightsProjects" SET "deletedAt" = NOW() WHERE id = $(id)`, {
      id: gone.id,
    })
    await qx.result(`UPDATE "insightsProjects" SET "enabled" = false WHERE id = $(id)`, {
      id: off.id,
    })

    expect(await findSharedDocsUrls(qx, a.id)).toEqual([])
  })

  test('is empty when the only user of a URL is the excluded project', async ({ qx }) => {
    const p = await createInsightsProject(qx, { name: 'solo', slug: 'solo', isLF: true })
    await upsertProjectDocDiscovery(qx, {
      projectId: p.id,
      docsUrl: 'https://solo.dev/docs',
      discoveryMethod: 'docs-path',
      confidence: 'medium',
      candidates: [],
    })
    expect(await findSharedDocsUrls(qx, p.id)).toEqual([])
  })
})
