import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  bulkInsertProjectCatalog,
  findProjectCatalogById,
  findProjectCatalogByRepoUrl,
  insertProjectCatalog,
  markProjectCatalogOnboardingSkipped,
  markProjectCatalogPreCheckSkipped,
  promoteProjectCatalogProvenance,
  updateProjectCatalog,
  upsertProjectCatalog,
  upsertProjectCatalogManualAction,
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

describe('markProjectCatalogPreCheckSkipped', () => {
  test('transitions a pending evaluate row to skip with the reason, leaving evaluationResult unset', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const updatedRows = await markProjectCatalogPreCheckSkipped(
      qx,
      inserted.id,
      'evaluation pre-check: repository already tracked in CDP',
    )

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(updatedRows).toBe(1)
    expect(row?.action).toBe('skip')
    expect(row?.skipReason).toBe('evaluation pre-check: repository already tracked in CDP')
    expect(row?.evaluationResult).toBeNull()
    expect(row?.evaluationReason).toBeNull()
    expect(row?.evaluatedAt).not.toBeNull()
  })

  test('clears a stale evaluationResult from a prior verdict on a manually re-queued row', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await updateProjectCatalog(qx, inserted.id, {
      evaluationResult: 'false',
      evaluationReason: 'not a real open source project',
    })

    const updatedRows = await markProjectCatalogPreCheckSkipped(
      qx,
      inserted.id,
      'evaluation pre-check: repository already tracked in CDP',
    )

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(updatedRows).toBe(1)
    expect(row?.action).toBe('skip')
    expect(row?.evaluationResult).toBeNull()
    expect(row?.evaluationReason).toBeNull()
  })

  test('does not touch a row already evaluated', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await updateProjectCatalog(qx, inserted.id, {
      action: 'onboard',
      evaluationResult: 'true',
      evaluationReason: null,
      evaluatedAt: new Date().toISOString(),
    })

    const updatedRows = await markProjectCatalogPreCheckSkipped(
      qx,
      inserted.id,
      'evaluation pre-check: repository already tracked in CDP',
    )

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(updatedRows).toBe(0)
    expect(row?.action).toBe('onboard')
    expect(row?.skipReason).toBeNull()
  })

  test('does not touch a row whose action is no longer evaluate', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'auto' }))

    const updatedRows = await markProjectCatalogPreCheckSkipped(qx, inserted.id, 'some reason')

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(updatedRows).toBe(0)
    expect(row?.action).toBe('auto')
    expect(row?.skipReason).toBeNull()
  })
})

describe('bulkInsertProjectCatalog', () => {
  test('inserts a row with action=skip and its skipReason', async ({ qx }) => {
    await bulkInsertProjectCatalog(qx, [
      catalogRow({
        action: 'skip',
        skipReason: 'repository already tracked in CDP (discovery pre-check)',
      }),
    ])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.action).toBe('skip')
    expect(row?.skipReason).toBe('repository already tracked in CDP (discovery pre-check)')
  })

  test('persists sourceUrl for a discovery row', async ({ qx }) => {
    await bulkInsertProjectCatalog(qx, [
      catalogRow({
        action: 'auto',
        source: 'insights-discussions',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
      }),
    ])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/42')
  })

  test('leaves sourceUrl null when the source provides none', async ({ qx }) => {
    await bulkInsertProjectCatalog(qx, [
      catalogRow({ action: 'auto', source: 'lf-criticality-score' }),
    ])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.sourceUrl).toBeNull()
  })

  test('does not blank an existing sourceUrl when the same repoUrl is re-sighted', async ({
    qx,
  }) => {
    await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        source: 'insights-discussions',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
      }),
    )

    await bulkInsertProjectCatalog(qx, [
      catalogRow({ action: 'auto', source: 'insights-discussions' }),
    ])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/42')
  })

  test('persists provenance for a discovery row', async ({ qx }) => {
    await bulkInsertProjectCatalog(qx, [
      catalogRow({
        action: 'auto',
        source: 'insights-discussions',
        provenance: 'github-discussion',
      }),
    ])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.provenance).toBe('github-discussion')
  })

  test('leaves provenance null when omitted', async ({ qx }) => {
    await bulkInsertProjectCatalog(qx, [catalogRow({ action: 'skip', skipReason: 'n/a' })])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.provenance).toBeNull()
  })

  test('rejects an unlisted provenance value', async ({ qx }) => {
    await expect(
      bulkInsertProjectCatalog(qx, [
        catalogRow({ action: 'auto', provenance: 'not-a-real-provenance' as never }),
      ]),
    ).rejects.toThrow(/projectCatalog_provenance_check/)
  })
})

describe('upsertProjectCatalog', () => {
  test('keeps the first sourceUrl when a later upsert carries a different one', async ({ qx }) => {
    await upsertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/1',
      }),
    )

    await upsertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/2',
      }),
    )

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/1')
  })

  test('backfills sourceUrl when the existing row has none', async ({ qx }) => {
    await upsertProjectCatalog(qx, catalogRow({ action: 'auto' }))

    await upsertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/1',
      }),
    )

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/1')
  })

  test('keeps the existing sourceUrl when the upsert omits it', async ({ qx }) => {
    await upsertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/1',
      }),
    )

    await upsertProjectCatalog(qx, catalogRow({ action: 'auto' }))

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/1')
  })

  test('keeps the existing provenance when a later upsert omits it', async ({ qx }) => {
    await upsertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        source: 'insights-discussions',
        provenance: 'github-discussion',
      }),
    )

    await upsertProjectCatalog(qx, catalogRow({ action: 'auto', source: 'insights-discussions' }))

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.provenance).toBe('github-discussion')
  })
})

describe('upsertProjectCatalogManualAction', () => {
  test('preserves the sourceUrl of a discovery row taken over manually', async ({ qx }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        source: 'insights-discussions',
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
      }),
    )

    const updated = await upsertProjectCatalogManualAction(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
      action: 'evaluate',
    })

    expect(updated?.source).toBe('manual')
    expect(updated?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/42')
  })

  test('leaves sourceUrl null for a manually created row', async ({ qx }) => {
    const row = catalogRow({ action: 'evaluate' })

    const created = await upsertProjectCatalogManualAction(qx, {
      projectSlug: row.projectSlug,
      repoName: row.repoName,
      repoUrl: row.repoUrl,
      action: row.action as 'evaluate',
    })

    expect(created?.source).toBe('manual')
    expect(created?.sourceUrl).toBeNull()
  })

  test('writes provenance while leaving source as manual', async ({ qx }) => {
    const row = catalogRow({ action: 'evaluate' })

    const created = await upsertProjectCatalogManualAction(qx, {
      projectSlug: row.projectSlug,
      repoName: row.repoName,
      repoUrl: row.repoUrl,
      action: row.action as 'evaluate',
      provenance: 'slack-tag',
    })

    expect(created?.source).toBe('manual')
    expect(created?.provenance).toBe('slack-tag')
  })

  test('replaces an existing bulk provenance with an explicit incoming one', async ({ qx }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        source: 'lf-criticality-score',
        provenance: 'lf-criticality-score',
      }),
    )

    const updated = await upsertProjectCatalogManualAction(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
      action: 'evaluate',
      provenance: 'slack-tag',
    })

    expect(updated?.source).toBe('manual')
    expect(updated?.provenance).toBe('slack-tag')
  })

  test('keeps the existing provenance when the manual action omits one', async ({ qx }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        source: 'lf-criticality-score',
        provenance: 'lf-criticality-score',
      }),
    )

    const updated = await upsertProjectCatalogManualAction(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
      action: 'evaluate',
    })

    expect(updated?.source).toBe('manual')
    expect(updated?.provenance).toBe('lf-criticality-score')
  })
})

describe('promoteProjectCatalogProvenance', () => {
  test('promotes a row with no provenance', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'auto' }))

    const affected = await promoteProjectCatalogProvenance(qx, 'github-discussion', [
      { repoUrl: inserted.repoUrl },
    ])

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(affected).toBe(1)
    expect(row?.provenance).toBe('github-discussion')
  })

  test('promotes a row with an existing bulk provenance', async ({ qx }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        source: 'lf-criticality-score',
        provenance: 'lf-criticality-score',
      }),
    )

    const affected = await promoteProjectCatalogProvenance(qx, 'github-discussion', [
      { repoUrl: inserted.repoUrl },
    ])

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(affected).toBe(1)
    expect(row?.provenance).toBe('github-discussion')
  })

  test('does not overwrite an existing human provenance from the other human channel', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        provenance: 'github-discussion',
      }),
    )

    const affected = await promoteProjectCatalogProvenance(qx, 'slack-tag', [
      { repoUrl: inserted.repoUrl },
    ])

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(affected).toBe(0)
    expect(row?.provenance).toBe('github-discussion')
  })

  test('fills a null sourceUrl but does not overwrite a populated one', async ({ qx }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'auto',
        source: 'lf-criticality-score',
        provenance: 'lf-criticality-score',
      }),
    )

    await promoteProjectCatalogProvenance(qx, 'github-discussion', [
      {
        repoUrl: inserted.repoUrl,
        sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/42',
      },
    ])

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(row?.sourceUrl).toBe('https://github.com/linuxfoundation/insights/discussions/42')
  })

  test('returns 0 for an empty refs array', async ({ qx }) => {
    const affected = await promoteProjectCatalogProvenance(qx, 'github-discussion', [])
    expect(affected).toBe(0)
  })
})
