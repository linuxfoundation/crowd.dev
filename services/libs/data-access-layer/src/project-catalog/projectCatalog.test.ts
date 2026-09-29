import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  bulkInsertProjectCatalog,
  claimProjectCatalogForOnboarding,
  claimProjectCatalogForSlackEvaluation,
  findProjectCatalogById,
  findProjectCatalogByRepoUrl,
  insertProjectCatalog,
  markProjectCatalogOnboardingSkipped,
  markProjectCatalogPreCheckSkipped,
  promoteProjectCatalogProvenance,
  setProjectCatalogSourceUrl,
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

describe('setProjectCatalogSourceUrl', () => {
  test('sets the sourceUrl', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow())

    await setProjectCatalogSourceUrl(qx, inserted.id, 'https://slack.test/archives/C1/p1')
    expect((await findProjectCatalogById(qx, inserted.id))?.sourceUrl).toBe(
      'https://slack.test/archives/C1/p1',
    )
  })
})

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

describe('claimProjectCatalogForSlackEvaluation', () => {
  test('claims a brand-new repo, inserting it as manual/evaluate', async ({ qx }) => {
    const row = catalogRow({ action: 'auto' })

    const claimed = await claimProjectCatalogForSlackEvaluation(qx, {
      projectSlug: row.projectSlug,
      repoName: row.repoName,
      repoUrl: row.repoUrl,
      provenance: 'slack-bot',
    })

    expect(claimed?.source).toBe('manual')
    expect(claimed?.action).toBe('evaluate')
    expect(claimed?.provenance).toBe('slack-bot')
  })

  test('resets a stale sourceUrl only when the row was already slack-bot', async ({ qx }) => {
    const slackRow = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'skip', provenance: 'slack-bot' }),
    )
    const otherRow = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'skip',
        repoUrl: 'https://github.com/foo/bar',
        provenance: 'github-discussion',
      }),
    )
    await setProjectCatalogSourceUrl(qx, slackRow.id, 'https://slack.test/old')
    await setProjectCatalogSourceUrl(qx, otherRow.id, 'https://github.com/foo/bar/discussions/1')

    for (const row of [slackRow, otherRow]) {
      await claimProjectCatalogForSlackEvaluation(qx, {
        projectSlug: row.projectSlug,
        repoName: row.repoName,
        repoUrl: row.repoUrl,
        provenance: 'slack-bot',
      })
    }

    expect((await findProjectCatalogById(qx, slackRow.id))?.sourceUrl).toBeNull()
    expect((await findProjectCatalogById(qx, otherRow.id))?.sourceUrl).toBe(
      'https://github.com/foo/bar/discussions/1',
    )
  })

  test('does not claim a row already onboarded', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow())
    await updateProjectCatalog(qx, inserted.id, {
      action: 'onboarded',
      onboardedAt: new Date().toISOString(),
    })

    const claimed = await claimProjectCatalogForSlackEvaluation(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
    })

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(claimed).toBeNull()
    expect(row?.action).toBe('onboarded')
  })

  test('does not claim a row already queued for evaluation, preventing a duplicate evaluateProject call', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const claimed = await claimProjectCatalogForSlackEvaluation(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
    })

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(claimed).toBeNull()
    expect(row?.action).toBe('evaluate')
  })

  test('re-claims a retriable skip row for evaluation', async ({ qx }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'skip',
        skipReason: 'evaluation pre-check: repository already tracked in CDP',
      }),
    )

    const claimed = await claimProjectCatalogForSlackEvaluation(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
    })

    expect(claimed?.action).toBe('evaluate')
    expect(claimed?.evaluatedAt).toBeNull()
  })

  test('re-claims a retriable error row for evaluation', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'error' }))

    const claimed = await claimProjectCatalogForSlackEvaluation(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
    })

    expect(claimed?.action).toBe('evaluate')
  })
})

describe('claimProjectCatalogForOnboarding', () => {
  test('claims a pending onboard row, stamping onboardedAt', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))

    const claimed = await claimProjectCatalogForOnboarding(qx, inserted.id)

    expect(claimed?.action).toBe('onboard')
    expect(claimed?.onboardedAt).not.toBeNull()
  })

  test('does not claim a row a second time', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))

    const firstClaim = await claimProjectCatalogForOnboarding(qx, inserted.id)
    const secondClaim = await claimProjectCatalogForOnboarding(qx, inserted.id)

    expect(firstClaim).not.toBeNull()
    expect(secondClaim).toBeNull()
  })

  test('does not claim a row whose action is no longer onboard', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const claimed = await claimProjectCatalogForOnboarding(qx, inserted.id)

    expect(claimed).toBeNull()
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
