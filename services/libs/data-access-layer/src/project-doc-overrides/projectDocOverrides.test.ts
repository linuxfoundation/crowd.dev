import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import { createInsightsProject } from '../collections'
import {
  createProjectDocOverride,
  deactivateProjectDocOverride,
  findActiveProjectDocOverride,
} from './projectDocOverrides'

const test = withQx(base)

describe('createProjectDocOverride', () => {
  test('creates an active override', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })

    const override = await createProjectDocOverride(qx, {
      projectId: project.id,
      docsUrl: 'https://kyverno.io/docs/',
      submittedBy: 'someone@example.com',
    })

    expect(override.active).toBe(true)
    expect(override.docsUrl).toBe('https://kyverno.io/docs/')
    expect((await findActiveProjectDocOverride(qx, project.id))?.id).toBe(override.id)
  })

  test('deactivates the previous override and keeps it as history', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'OpenFGA',
      slug: 'openfga',
      isLF: true,
    })

    const first = await createProjectDocOverride(qx, {
      projectId: project.id,
      docsUrl: 'https://openfga.dev/docs',
      submittedBy: 'a@example.com',
    })
    const second = await createProjectDocOverride(qx, {
      projectId: project.id,
      docsUrl: 'https://openfga.dev/docs/latest',
      submittedBy: 'b@example.com',
    })

    expect((await findActiveProjectDocOverride(qx, project.id))?.id).toBe(second.id)

    const rows = await qx.select(
      `SELECT "id", "active" FROM "projectDocOverrides" WHERE "projectId" = $(projectId) ORDER BY "submittedAt"`,
      { projectId: project.id },
    )
    expect(rows).toEqual([
      { id: first.id, active: false },
      { id: second.id, active: true },
    ])
  })
})

describe('deactivateProjectDocOverride', () => {
  test('returns the deactivated row and leaves no active override', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })
    const override = await createProjectDocOverride(qx, {
      projectId: project.id,
      docsUrl: 'https://kyverno.io/docs/',
      submittedBy: 'someone@example.com',
    })

    const deactivated = await deactivateProjectDocOverride(qx, project.id)

    expect(deactivated?.id).toBe(override.id)
    expect(deactivated?.active).toBe(false)
    expect(await findActiveProjectDocOverride(qx, project.id)).toBeNull()
  })

  test('returns null when there is nothing active', async ({ qx }) => {
    const project = await createInsightsProject(qx, {
      name: 'Kyverno',
      slug: 'kyverno',
      isLF: true,
    })

    expect(await deactivateProjectDocOverride(qx, project.id)).toBeNull()
  })
})
