import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  findProjectCatalogById,
  insertProjectCatalog,
  markProjectCatalogOnboardingSkipped,
  updateProjectCatalog,
} from './projectCatalog'

const test = withQx(base)

function catalogRow(overrides: Partial<Parameters<typeof insertProjectCatalog>[1]> = {}) {
  return {
    projectSlug: 'gerritcodereview-gerrit',
    repoName: 'gerrit',
    repoUrl: 'https://github.com/gerritcodereview/gerrit',
    action: 'onboard' as const,
    ...overrides,
  }
}

describe('markProjectCatalogOnboardingSkipped', () => {
  test('transitions a pending row to skip with the reason, and clears a prior onboarding error', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow())
    await updateProjectCatalog(qx, inserted.id, {
      onboardingError: 'Segment creation returned HTTP 500: Internal Server Error',
    })

    const updatedRows = await markProjectCatalogOnboardingSkipped(
      qx,
      inserted.id,
      "Insights project 'gerritcodereview-gerrit' was deleted on 2026-04-10; onboarding skipped for manual review",
    )

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(updatedRows).toBe(1)
    expect(row?.action).toBe('skip')
    expect(row?.skipReason).toBe(
      "Insights project 'gerritcodereview-gerrit' was deleted on 2026-04-10; onboarding skipped for manual review",
    )
    expect(row?.onboardingError).toBeNull()
  })

  test('does not touch a row whose action is no longer onboard', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'error' }))

    const updatedRows = await markProjectCatalogOnboardingSkipped(qx, inserted.id, 'some reason')

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(updatedRows).toBe(0)
    expect(row?.action).toBe('error')
    expect(row?.skipReason).toBeNull()
  })

  test('does not touch a row already onboarded', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow())
    await updateProjectCatalog(qx, inserted.id, {
      action: 'onboarded',
      onboardedAt: new Date().toISOString(),
    })

    const updatedRows = await markProjectCatalogOnboardingSkipped(qx, inserted.id, 'some reason')

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(updatedRows).toBe(0)
    expect(row?.action).toBe('onboarded')
    expect(row?.skipReason).toBeNull()
  })
})
