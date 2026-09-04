import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  InsightsProjectField,
  createInsightsProject,
  deleteInsightsProject,
  findInsightsProjectBySlugIncludingDeleted,
} from './index'

const test = withQx(base)

describe('findInsightsProjectBySlugIncludingDeleted', () => {
  test('finds a soft-deleted row by slug, with deletedAt set', async ({ qx }) => {
    const created = await createInsightsProject(qx, {
      name: 'Gerrit',
      slug: 'gerritcodereview-gerrit',
    })
    await deleteInsightsProject(qx, created.id)

    const found = await findInsightsProjectBySlugIncludingDeleted(qx, 'gerritcodereview-gerrit', [
      InsightsProjectField.ID,
      InsightsProjectField.DELETED_AT,
    ])

    expect(found?.id).toBe(created.id)
    expect(found?.deletedAt).not.toBeNull()
  })

  test('finds a live row by slug, with deletedAt null', async ({ qx }) => {
    const created = await createInsightsProject(qx, {
      name: 'Kubernetes',
      slug: 'kubernetes-kubernetes',
    })

    const found = await findInsightsProjectBySlugIncludingDeleted(qx, 'kubernetes-kubernetes', [
      InsightsProjectField.ID,
      InsightsProjectField.DELETED_AT,
    ])

    expect(found?.id).toBe(created.id)
    expect(found?.deletedAt).toBeNull()
  })

  test('returns null when no row has the slug', async ({ qx }) => {
    const found = await findInsightsProjectBySlugIncludingDeleted(qx, 'does-not-exist', [
      InsightsProjectField.ID,
    ])

    expect(found).toBeNull()
  })
})
