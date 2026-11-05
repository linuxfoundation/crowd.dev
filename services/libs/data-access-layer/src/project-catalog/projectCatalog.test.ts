import { test as base, describe, expect } from 'vitest'

import { withQx } from '@crowd/test-kit/db'

import {
  bulkInsertProjectCatalog,
  countProjectCatalogOpenBySourceUrl,
  claimProjectCatalogForForcedOnboarding,
  claimProjectCatalogForOnboarding,
  claimProjectCatalogForSlackEvaluation,
  finalizeProjectCatalogEvaluation,
  deferProjectCatalogEvaluation,
  markProjectCatalogEvaluationFailed,
  requeueProjectCatalogEvaluation,
  findProjectCatalogById,
  findProjectCatalogByRepoUrl,
  findProjectCatalogByRepoUrls,
  findProjectCatalogPendingEvaluation,
  findProjectCatalogPendingOnboarding,
  findProjectCatalogPendingReview,
  findProjectCatalogReviewWithoutMessage,
  insertProjectCatalog,
  isProjectCatalogOnboardingPending,
  markProjectCatalogClassified,
  markProjectCatalogOnboardingFailed,
  markProjectCatalogOnboardingSkipped,
  markProjectCatalogPreCheckSkipped,
  promoteProjectCatalogIdsToEvaluate,
  promoteProjectCatalogProvenance,
  releaseProjectCatalogOnboardingClaim,
  lockProjectCatalogReviewRequest,
  reopenRejectedProjectCatalog,
  resolveProjectCatalogReview,
  setProjectCatalogReviewMessageTs,
  setProjectCatalogSourceUrl,
  updateProjectCatalog,
  upsertProjectCatalog,
  upsertProjectCatalogManualAction,
} from './projectCatalog'

const test = withQx(base)

function staleClaimTimestamp() {
  return new Date(Date.now() - 31 * 60 * 1000).toISOString()
}

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
  test('sets the sourceUrl when it is empty', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow())

    await setProjectCatalogSourceUrl(qx, inserted.id, 'https://slack.test/archives/C1/p1')
    expect((await findProjectCatalogById(qx, inserted.id))?.sourceUrl).toBe(
      'https://slack.test/archives/C1/p1',
    )
  })

  test('never overwrites an existing sourceUrl', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow())
    await setProjectCatalogSourceUrl(qx, inserted.id, 'https://github.com/foo/bar/discussions/1')

    await setProjectCatalogSourceUrl(qx, inserted.id, 'https://slack.test/archives/C1/p1')

    expect((await findProjectCatalogById(qx, inserted.id))?.sourceUrl).toBe(
      'https://github.com/foo/bar/discussions/1',
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

  test('persists requestText and projectName', async ({ qx }) => {
    await bulkInsertProjectCatalog(qx, [
      catalogRow({
        action: 'auto',
        requestText: 'Please onboard Gerrit https://github.com/gerritcodereview/gerrit',
        projectName: 'Gerrit',
      }),
    ])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.requestText).toBe(
      'Please onboard Gerrit https://github.com/gerritcodereview/gerrit',
    )
    expect(row?.projectName).toBe('Gerrit')
  })

  test('leaves requestText and projectName null when omitted', async ({ qx }) => {
    await bulkInsertProjectCatalog(qx, [catalogRow({ action: 'auto' })])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.requestText).toBeNull()
    expect(row?.projectName).toBeNull()
  })

  test('returns the id and repoUrl of the rows it inserted, skipping conflicts', async ({ qx }) => {
    await insertProjectCatalog(qx, catalogRow({ action: 'auto' }))

    const inserted = await bulkInsertProjectCatalog(qx, [
      catalogRow({ action: 'auto' }),
      catalogRow({
        action: 'auto',
        projectSlug: 'acme-one',
        repoName: 'one',
        repoUrl: 'https://github.com/acme/one',
      }),
    ])

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/acme/one')
    expect(inserted).toEqual([{ id: row?.id, repoUrl: 'https://github.com/acme/one' }])
  })

  test('returns an empty list for an empty input', async ({ qx }) => {
    expect(await bulkInsertProjectCatalog(qx, [])).toEqual([])
  })
})

describe('promoteProjectCatalogIdsToEvaluate', () => {
  test('moves only the given auto rows to evaluate', async ({ qx }) => {
    const auto = await insertProjectCatalog(qx, catalogRow({ action: 'auto' }))
    const other = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'auto', repoUrl: 'https://github.com/acme/one' }),
    )
    const skipped = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'skip', skipReason: 'n/a', repoUrl: 'https://github.com/acme/two' }),
    )

    const promoted = await promoteProjectCatalogIdsToEvaluate(qx, [auto.id, skipped.id])

    expect(promoted).toBe(1)
    expect((await findProjectCatalogById(qx, auto.id))?.action).toBe('evaluate')
    expect((await findProjectCatalogById(qx, other.id))?.action).toBe('auto')
    expect((await findProjectCatalogById(qx, skipped.id))?.action).toBe('skip')
  })

  test('returns 0 for an empty list', async ({ qx }) => {
    expect(await promoteProjectCatalogIdsToEvaluate(qx, [])).toBe(0)
  })
})

describe('pending queues filtered by ids', () => {
  test('findProjectCatalogPendingEvaluation returns only the given evaluate rows', async ({
    qx,
  }) => {
    const wanted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await insertProjectCatalog(
      qx,
      catalogRow({ action: 'evaluate', repoUrl: 'https://github.com/acme/one' }),
    )
    const auto = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'auto', repoUrl: 'https://github.com/acme/two' }),
    )

    const rows = await findProjectCatalogPendingEvaluation(qx, { ids: [wanted.id, auto.id] })

    expect(rows.map((row) => row.id)).toEqual([wanted.id])
  })

  test('findProjectCatalogPendingOnboarding returns only the given onboard rows', async ({
    qx,
  }) => {
    const wanted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))
    await insertProjectCatalog(
      qx,
      catalogRow({ action: 'onboard', repoUrl: 'https://github.com/acme/one' }),
    )

    const rows = await findProjectCatalogPendingOnboarding(qx, { ids: [wanted.id] })

    expect(rows.map((row) => row.id)).toEqual([wanted.id])
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

  test('backfills requestText and projectName but never overwrites them', async ({ qx }) => {
    await upsertProjectCatalog(qx, catalogRow({ action: 'auto' }))

    await upsertProjectCatalog(
      qx,
      catalogRow({ action: 'auto', requestText: 'first request', projectName: 'Gerrit' }),
    )
    await upsertProjectCatalog(
      qx,
      catalogRow({ action: 'auto', requestText: 'second request', projectName: 'Other' }),
    )

    const row = await findProjectCatalogByRepoUrl(qx, 'https://github.com/gerritcodereview/gerrit')
    expect(row?.requestText).toBe('first request')
    expect(row?.projectName).toBe('Gerrit')
  })

  test('keeps a row in review when a later upsert asks for evaluate', async ({ qx }) => {
    await insertProjectCatalog(qx, catalogRow({ action: 'review' }))

    const row = await upsertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    expect(row.action).toBe('review')
  })
})

describe('markProjectCatalogClassified', () => {
  test('stores the classification and routes the row to the given action', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const row = await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'lf_in_cdp',
      proposedAction: 'update_integration',
      pccProjectId: 'a0941000002wBz4AAE',
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'review',
      skipReason: null,
      reviewReason: 'automatic onboarding is disabled',
    })

    expect(row?.action).toBe('review')
    expect(row?.classificationKind).toBe('lf_in_cdp')
    expect(row?.proposedAction).toBe('update_integration')
    expect(row?.pccProjectId).toBe('a0941000002wBz4AAE')
    expect(row?.reviewReason).toBe('automatic onboarding is disabled')
    expect(row?.projectName).toBe('Widget')
    expect(row?.classifiedAt).not.toBeNull()
    expect(row?.evaluatedAt).toBeNull()
  })

  test('routes a non-LF project back to evaluate without a proposed action', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const row = await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'non_lf_new_project',
      proposedAction: null,
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'evaluate',
      skipReason: null,
      reviewReason: null,
    })

    expect(row?.action).toBe('evaluate')
    expect(row?.proposedAction).toBeNull()
    expect(row?.classifiedAt).not.toBeNull()
  })

  test('skips with a reason', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const row = await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'already_tracked',
      proposedAction: null,
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'skip',
      skipReason: 'repository already tracked in CDP',
      reviewReason: null,
    })

    expect(row?.action).toBe('skip')
    expect(row?.skipReason).toBe('repository already tracked in CDP')
  })

  test('does not classify a row a second time', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    const classification = {
      projectName: null,
      classificationKind: 'non_lf_new_project' as const,
      proposedAction: null,
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'evaluate' as const,
      skipReason: null,
      reviewReason: null,
    }

    const first = await markProjectCatalogClassified(qx, inserted.id, classification)
    const second = await markProjectCatalogClassified(qx, inserted.id, {
      ...classification,
      classificationKind: 'ambiguous',
    })

    expect(first).not.toBeNull()
    expect(second).toBeNull()
    expect((await findProjectCatalogById(qx, inserted.id))?.classificationKind).toBe(
      'non_lf_new_project',
    )
  })

  test('does not touch a row whose action is no longer evaluate', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'auto' }))

    const row = await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'ambiguous',
      proposedAction: 'human_review',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'review',
      skipReason: null,
      reviewReason: null,
    })

    expect(row).toBeNull()
    expect((await findProjectCatalogById(qx, inserted.id))?.action).toBe('auto')
  })

  test('classifies again a row manually re-queued for evaluation', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'already_tracked',
      proposedAction: null,
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'skip',
      skipReason: 'repository already tracked in CDP',
      reviewReason: null,
    })

    await upsertProjectCatalogManualAction(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
      action: 'evaluate',
    })
    const row = await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'non_lf_new_project',
      proposedAction: null,
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'evaluate',
      skipReason: null,
      reviewReason: null,
    })

    expect(row?.classificationKind).toBe('non_lf_new_project')
  })

  test('clears the previous review cycle when classifying a re-queued row', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'ambiguous',
      proposedAction: 'human_review',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'review',
      skipReason: null,
      reviewReason: null,
    })
    await setProjectCatalogReviewMessageTs(qx, inserted.id, '1700000000.000100')
    await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'reject',
      reviewedBy: 'U1',
      decisionReason: 'wrong_project',
      decisionNote: 'belongs elsewhere',
    })

    await upsertProjectCatalogManualAction(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
      action: 'evaluate',
    })
    const row = await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'ambiguous',
      proposedAction: 'human_review',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'review',
      skipReason: null,
      reviewReason: null,
    })

    expect(row?.action).toBe('review')
    expect(row?.reviewedAt).toBeNull()
    expect(row?.reviewedBy).toBeNull()
    expect(row?.reviewMessageTs).toBeNull()
    expect(row?.decisionReason).toBeNull()
    expect(row?.decisionNote).toBeNull()
  })

  test('rejects an unlisted classification kind', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    await expect(
      markProjectCatalogClassified(qx, inserted.id, {
        projectName: 'Widget',
        classificationKind: 'not-a-kind' as never,
        proposedAction: null,
        pccProjectId: null,
        matchedProjectName: null,
        matchedSegmentId: null,
        action: 'evaluate',
        skipReason: null,
        reviewReason: null,
      }),
    ).rejects.toThrow(/projectCatalog_classificationKind_check/)
  })
})

describe('findProjectCatalogPendingReview', () => {
  test('returns only rows in review, oldest classification first', async ({ qx }) => {
    const later = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'review', repoUrl: 'https://github.com/foo/later' }),
    )
    const earlier = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))
    await insertProjectCatalog(
      qx,
      catalogRow({ action: 'evaluate', repoUrl: 'https://github.com/foo/evaluate' }),
    )
    await qx.selectNone(
      `UPDATE "projectCatalog" SET "classifiedAt" = NOW() - INTERVAL '1 day' WHERE id = $(id)`,
      { id: earlier.id },
    )
    await qx.selectNone(`UPDATE "projectCatalog" SET "classifiedAt" = NOW() WHERE id = $(id)`, {
      id: later.id,
    })

    const rows = await findProjectCatalogPendingReview(qx)

    expect(rows.map((row) => row.id)).toEqual([earlier.id, later.id])
  })
})

function reviewClassification(
  proposedAction: 'human_review' | 'update_integration',
): Parameters<typeof markProjectCatalogClassified>[2] {
  return {
    projectName: 'Gerrit',
    classificationKind: 'lf_in_cdp',
    proposedAction,
    pccProjectId: 'pcc-1',
    matchedProjectName: 'Gerrit',
    matchedSegmentId: null,
    action: 'review',
    skipReason: null,
    reviewReason: 'needs a check',
  }
}

describe('evaluation failures and retries', () => {
  test('marks a row in evaluate as failed and requeues it for a retry', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    expect(await markProjectCatalogEvaluationFailed(qx, inserted.id, 'API returned HTTP 500')).toBe(
      1,
    )
    const failed = await findProjectCatalogById(qx, inserted.id)
    expect(failed?.action).toBe('error')
    expect(failed?.evaluationResult).toBe('error')
    expect(failed?.evaluationReason).toBe('API returned HTTP 500')
    expect(failed?.evaluatedAt).not.toBeNull()

    const requeued = await requeueProjectCatalogEvaluation(qx, inserted.id)
    expect(requeued?.action).toBe('evaluate')
    expect(requeued?.evaluationResult).toBeNull()
    expect(requeued?.evaluationReason).toBeNull()
    expect(requeued?.evaluatedAt).toBeNull()
  })

  test('defers a transient failure to the next run until the last allowed run', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const defer = (expectedAttempts: number, reason: string) =>
      deferProjectCatalogEvaluation(qx, inserted.id, { reason, expectedAttempts, maxRuns: 3 })

    expect(await defer(0, 'API returned HTTP 503')).toMatchObject({
      action: 'evaluate',
      evaluationAttempts: 1,
      evaluationReason: 'API returned HTTP 503',
      evaluatedAt: null,
    })
    expect(await defer(0, 'redelivered')).toBeNull()
    expect(await defer(1, 'again')).toMatchObject({ evaluationAttempts: 2 })
    expect(await defer(2, 'and again')).toBeNull()

    expect(await markProjectCatalogEvaluationFailed(qx, inserted.id, 'stale delivery', 1)).toBe(0)
    expect(await markProjectCatalogEvaluationFailed(qx, inserted.id, 'and again', 2)).toBe(1)

    const requeued = await requeueProjectCatalogEvaluation(qx, inserted.id)
    expect(requeued?.evaluationAttempts).toBe(0)
  })

  test('never touches rows outside evaluate or outside an evaluation error', async ({ qx }) => {
    const onboarded = await insertProjectCatalog(qx, catalogRow({ action: 'onboarded' }))
    const onboardingError = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'error', repoUrl: 'https://github.com/gerritcodereview/other' }),
    )

    expect(await markProjectCatalogEvaluationFailed(qx, onboarded.id, 'x')).toBe(0)
    expect(await requeueProjectCatalogEvaluation(qx, onboarded.id)).toBeNull()
    expect(await requeueProjectCatalogEvaluation(qx, onboardingError.id)).toBeNull()
  })

  test('never requeues an onboarding failure that followed an earlier check error', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogEvaluationFailed(qx, inserted.id, 'API returned HTTP 500')
    await updateProjectCatalog(qx, inserted.id, { onboardingError: 'integration call failed' })

    expect(await requeueProjectCatalogEvaluation(qx, inserted.id)).toBeNull()
  })
})

describe('finalizeProjectCatalogEvaluation', () => {
  test('stores the review reason when the verdict routes the row to review', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const row = await finalizeProjectCatalogEvaluation(qx, inserted.id, {
      action: 'review',
      evaluationResult: 'unsure',
      evaluationReason: 'mixed signals',
      reviewReason: "the automatic check couldn't decide: mixed signals",
    })

    expect(row?.action).toBe('review')
    expect(row?.reviewReason).toBe("the automatic check couldn't decide: mixed signals")
    expect(row?.evaluatedAt).not.toBeNull()
  })

  test('keeps the existing review reason when the verdict carries none', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const row = await finalizeProjectCatalogEvaluation(qx, inserted.id, {
      action: 'onboard',
      evaluationResult: 'onboard',
      evaluationReason: null,
    })

    expect(row?.action).toBe('onboard')
    expect(row?.reviewReason).toBeNull()
  })
})

describe('resolveProjectCatalogReview', () => {
  test('stores the rejection reason and note', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogClassified(qx, inserted.id, reviewClassification('update_integration'))

    const row = await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'reject',
      reviewedBy: 'U123',
      decisionReason: 'archived_or_fork',
      decisionNote: 'Example sites are not tracked.',
    })

    expect(row?.action).toBe('skip')
    expect(row?.skipReason).toBe('rejected in review by U123')
    expect(row?.decisionReason).toBe('archived_or_fork')
    expect(row?.decisionNote).toBe('Example sites are not tracked.')
  })

  test('done marks a human-review row as onboarded by hand', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogClassified(qx, inserted.id, reviewClassification('human_review'))

    const row = await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'done',
      reviewedBy: 'U123',
    })

    expect(row?.action).toBe('onboarded')
    expect(row?.onboardedAt).not.toBeNull()
    expect(row?.reviewedBy).toBe('U123')
    expect(row?.skipReason).toBeNull()
  })

  test('refuses approve on a human-review row and done on an automatic one', async ({ qx }) => {
    const manual = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogClassified(qx, manual.id, reviewClassification('human_review'))
    const automatic = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'evaluate', repoUrl: 'https://github.com/gerritcodereview/other' }),
    )
    await markProjectCatalogClassified(qx, automatic.id, reviewClassification('update_integration'))

    expect(
      await resolveProjectCatalogReview(qx, manual.id, { decision: 'approve', reviewedBy: 'U1' }),
    ).toBeNull()
    expect(
      await resolveProjectCatalogReview(qx, automatic.id, { decision: 'done', reviewedBy: 'U1' }),
    ).toBeNull()
    expect((await findProjectCatalogById(qx, manual.id))?.action).toBe('review')
    expect((await findProjectCatalogById(qx, automatic.id))?.action).toBe('review')
  })

  test('a row without a proposed action can only be marked done', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))

    expect(
      await resolveProjectCatalogReview(qx, inserted.id, { decision: 'approve', reviewedBy: 'U1' }),
    ).toBeNull()
    const row = await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'done',
      reviewedBy: 'U1',
    })

    expect(row?.action).toBe('onboarded')
    expect(row?.onboardedAt).not.toBeNull()
  })

  test('approve moves the row to onboard and records the reviewer', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogClassified(qx, inserted.id, reviewClassification('update_integration'))

    const row = await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'approve',
      reviewedBy: 'U123',
    })

    expect(row?.action).toBe('onboard')
    expect(row?.reviewedBy).toBe('U123')
    expect(row?.reviewedAt).not.toBeNull()
    expect(row?.skipReason).toBeNull()
    expect(row?.onboardedAt).toBeNull()
  })

  test('reject moves the row to skip with a reason naming the reviewer', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))

    const row = await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'reject',
      reviewedBy: 'U123',
    })

    expect(row?.action).toBe('skip')
    expect(row?.skipReason).toBe('rejected in review by U123')
    expect(row?.reviewedBy).toBe('U123')
  })

  test('does not resolve a row a second time', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogClassified(qx, inserted.id, reviewClassification('update_integration'))

    await resolveProjectCatalogReview(qx, inserted.id, { decision: 'approve', reviewedBy: 'U1' })
    const second = await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'reject',
      reviewedBy: 'U2',
    })

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(second).toBeNull()
    expect(row?.action).toBe('onboard')
    expect(row?.reviewedBy).toBe('U1')
  })

  test('does not touch a row that is not in review', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))

    const row = await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'approve',
      reviewedBy: 'U123',
    })

    expect(row).toBeNull()
    expect((await findProjectCatalogById(qx, inserted.id))?.action).toBe('evaluate')
  })
})

describe('lockProjectCatalogReviewRequest', () => {
  test('locks a review row without a message inside a transaction', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))

    const locked = await qx.tx((tx) => lockProjectCatalogReviewRequest(tx, inserted.id))

    expect(locked).toBe(true)
  })

  test('does not lock a row that already has a message or left review', async ({ qx }) => {
    const withMessage = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))
    await setProjectCatalogReviewMessageTs(qx, withMessage.id, '1700000000.000100')
    const onboard = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'onboard', repoUrl: 'https://github.com/gerritcodereview/plugins' }),
    )

    expect(await qx.tx((tx) => lockProjectCatalogReviewRequest(tx, withMessage.id))).toBe(false)
    expect(await qx.tx((tx) => lockProjectCatalogReviewRequest(tx, onboard.id))).toBe(false)
  })
})

describe('setProjectCatalogReviewMessageTs', () => {
  test('stores the Slack message ts on a row in review', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))

    const updatedRows = await setProjectCatalogReviewMessageTs(qx, inserted.id, '1700000000.000100')

    expect(updatedRows).toBe(1)
    expect((await findProjectCatalogById(qx, inserted.id))?.reviewMessageTs).toBe(
      '1700000000.000100',
    )
  })

  test('does not touch a row that is not in review', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))

    const updatedRows = await setProjectCatalogReviewMessageTs(qx, inserted.id, '1700000000.000100')

    expect(updatedRows).toBe(0)
    expect((await findProjectCatalogById(qx, inserted.id))?.reviewMessageTs).toBeNull()
  })

  test('keeps the first message ts when a second request is posted', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))
    await setProjectCatalogReviewMessageTs(qx, inserted.id, '1700000000.000100')

    const updatedRows = await setProjectCatalogReviewMessageTs(qx, inserted.id, '1700000000.000200')

    expect(updatedRows).toBe(0)
    expect((await findProjectCatalogById(qx, inserted.id))?.reviewMessageTs).toBe(
      '1700000000.000100',
    )
  })
})

describe('findProjectCatalogReviewWithoutMessage', () => {
  test('returns only rows in review that have no Slack message yet', async ({ qx }) => {
    const pending = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))
    const posted = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'review', repoUrl: 'https://github.com/foo/posted' }),
    )
    await setProjectCatalogReviewMessageTs(qx, posted.id, '1700000000.000100')
    await insertProjectCatalog(
      qx,
      catalogRow({ action: 'evaluate', repoUrl: 'https://github.com/foo/evaluate' }),
    )

    const rows = await findProjectCatalogReviewWithoutMessage(qx)

    expect(rows.map((row) => row.id)).toEqual([pending.id])
  })

  test('restricts the rows to the requested ids', async ({ qx }) => {
    const requested = await insertProjectCatalog(qx, catalogRow({ action: 'review' }))
    await insertProjectCatalog(
      qx,
      catalogRow({ action: 'review', repoUrl: 'https://github.com/foo/other' }),
    )

    const rows = await findProjectCatalogReviewWithoutMessage(qx, { ids: [requested.id] })

    expect(rows.map((row) => row.id)).toEqual([requested.id])
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

  test('stores the project name and request text of a manually created row', async ({ qx }) => {
    const row = catalogRow({ action: 'auto' })

    const created = await upsertProjectCatalogManualAction(qx, {
      projectSlug: row.projectSlug,
      repoName: row.repoName,
      repoUrl: row.repoUrl,
      action: 'auto',
      projectName: 'Gerrit',
      requestText: 'Please onboard gerrit, it is an LF project',
    })

    expect(created?.projectName).toBe('Gerrit')
    expect(created?.requestText).toBe('Please onboard gerrit, it is an LF project')
  })

  test('keeps the existing project name and request text when the manual action omits them', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'auto', projectName: 'Gerrit', requestText: 'original request' }),
    )

    const updated = await upsertProjectCatalogManualAction(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
      action: 'evaluate',
    })

    expect(updated?.projectName).toBe('Gerrit')
    expect(updated?.requestText).toBe('original request')
  })

  test('replaces the existing project name and request text with explicit incoming ones', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'auto', projectName: 'Gerrit', requestText: 'original request' }),
    )

    const updated = await upsertProjectCatalogManualAction(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
      action: 'evaluate',
      projectName: 'Gerrit Code Review',
      requestText: 'updated request',
    })

    expect(updated?.projectName).toBe('Gerrit Code Review')
    expect(updated?.requestText).toBe('updated request')
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

  test('resets a stale Slack permalink but keeps other sourceUrls across repeated claims', async ({
    qx,
  }) => {
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
    await setProjectCatalogSourceUrl(qx, slackRow.id, 'https://acme.slack.com/archives/C1/p1')
    await setProjectCatalogSourceUrl(qx, otherRow.id, 'https://github.com/foo/bar/discussions/1')

    for (const row of [slackRow, otherRow]) {
      for (let attempt = 0; attempt < 2; attempt++) {
        await claimProjectCatalogForSlackEvaluation(qx, {
          projectSlug: row.projectSlug,
          repoName: row.repoName,
          repoUrl: row.repoUrl,
          provenance: 'slack-bot',
        })
        await updateProjectCatalog(qx, row.id, { action: 'skip' })
      }
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

  test('clears a prior classification stamp so the row gets classified again', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'evaluate' }))
    await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Widget',
      classificationKind: 'already_tracked',
      proposedAction: null,
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'skip',
      skipReason: 'repository already tracked in CDP',
      reviewReason: null,
    })

    const claimed = await claimProjectCatalogForSlackEvaluation(qx, {
      projectSlug: inserted.projectSlug,
      repoName: inserted.repoName,
      repoUrl: inserted.repoUrl,
    })

    expect(claimed?.action).toBe('evaluate')
    expect(claimed?.classifiedAt).toBeNull()
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

  test('reclaims a row whose claim was never released by a dead run', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))
    await updateProjectCatalog(qx, inserted.id, { onboardedAt: staleClaimTimestamp() })

    const pending = await findProjectCatalogPendingOnboarding(qx, { ids: [inserted.id] })
    const claimed = await claimProjectCatalogForOnboarding(qx, inserted.id)

    expect(pending.map((row) => row.id)).toEqual([inserted.id])
    expect(claimed).not.toBeNull()
    expect(new Date(claimed!.onboardedAt!).getTime()).toBeGreaterThan(Date.now() - 60_000)
  })

  test('lets a stale claim be marked as failed', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))
    await updateProjectCatalog(qx, inserted.id, { onboardedAt: staleClaimTimestamp() })

    const updatedRows = await markProjectCatalogOnboardingFailed(qx, inserted.id, 'worker died')
    const row = await findProjectCatalogById(qx, inserted.id)

    expect(updatedRows).toBe(1)
    expect(row?.action).toBe('error')
    expect(row?.onboardedAt).toBeNull()
  })

  test('lets a stale claim be skipped and clears the claim', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))
    await updateProjectCatalog(qx, inserted.id, { onboardedAt: staleClaimTimestamp() })

    const updatedRows = await markProjectCatalogOnboardingSkipped(qx, inserted.id, 'gone')
    const row = await findProjectCatalogById(qx, inserted.id)

    expect(updatedRows).toBe(1)
    expect(row?.action).toBe('skip')
    expect(row?.onboardedAt).toBeNull()
  })
})

describe('releaseProjectCatalogOnboardingClaim', () => {
  test('clears onboardedAt so the row can be claimed again', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))
    await claimProjectCatalogForOnboarding(qx, inserted.id)

    const released = await releaseProjectCatalogOnboardingClaim(qx, inserted.id)
    const reclaimed = await claimProjectCatalogForOnboarding(qx, inserted.id)

    expect(released).toBe(1)
    expect(reclaimed).not.toBeNull()
  })

  test('does not touch a row that was onboarded', async ({ qx }) => {
    const inserted = await insertProjectCatalog(qx, catalogRow({ action: 'onboard' }))
    await claimProjectCatalogForOnboarding(qx, inserted.id)
    await updateProjectCatalog(qx, inserted.id, { action: 'onboarded' })

    const released = await releaseProjectCatalogOnboardingClaim(qx, inserted.id)
    const current = await findProjectCatalogById(qx, inserted.id)

    expect(released).toBe(0)
    expect(current?.onboardedAt).not.toBeNull()
  })
})

describe('claimProjectCatalogForForcedOnboarding', () => {
  async function insertEvaluated(
    qx: Parameters<typeof insertProjectCatalog>[0],
    {
      provenance = 'slack-bot',
      action = 'skip',
      evaluationResult = 'false',
    }: {
      provenance?: 'slack-bot' | 'github-discussion'
      action?: 'skip' | 'unsure' | 'onboard'
      evaluationResult?: string | null
    } = {},
  ) {
    const inserted = await insertProjectCatalog(qx, catalogRow({ provenance, action }))
    await updateProjectCatalog(qx, inserted.id, { evaluationResult })
    return inserted
  }

  test('claims a negatively evaluated slack-bot row, flipping it to onboard', async ({ qx }) => {
    const inserted = await insertEvaluated(qx)

    const claimed = await claimProjectCatalogForForcedOnboarding(qx, inserted.id)

    expect(claimed?.action).toBe('onboard')
    expect(claimed?.onboardedAt).not.toBeNull()
  })

  test('claims an unsure row that has a real evaluation result', async ({ qx }) => {
    const inserted = await insertEvaluated(qx, { action: 'unsure' })

    expect(await claimProjectCatalogForForcedOnboarding(qx, inserted.id)).not.toBeNull()
  })

  test('does not claim a row a second time', async ({ qx }) => {
    const inserted = await insertEvaluated(qx)

    await claimProjectCatalogForForcedOnboarding(qx, inserted.id)

    expect(await claimProjectCatalogForForcedOnboarding(qx, inserted.id)).toBeNull()
  })

  test('does not claim a row whose evaluation errored', async ({ qx }) => {
    const inserted = await insertEvaluated(qx, { action: 'unsure', evaluationResult: 'error' })

    expect(await claimProjectCatalogForForcedOnboarding(qx, inserted.id)).toBeNull()
  })

  test('does not claim a row skipped by the pre-check', async ({ qx }) => {
    const inserted = await insertEvaluated(qx, { evaluationResult: null })

    expect(await claimProjectCatalogForForcedOnboarding(qx, inserted.id)).toBeNull()
  })

  test('does not claim rows from other provenances', async ({ qx }) => {
    const inserted = await insertEvaluated(qx, { provenance: 'github-discussion' })

    expect(await claimProjectCatalogForForcedOnboarding(qx, inserted.id)).toBeNull()
  })

  test('does not claim a row that is not skipped or unsure', async ({ qx }) => {
    const inserted = await insertEvaluated(qx, { action: 'onboard' })

    expect(await claimProjectCatalogForForcedOnboarding(qx, inserted.id)).toBeNull()
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

  test('adopts a pending row into an organization request with its context, to be classified again', async ({
    qx,
  }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'evaluate',
        source: 'lf-criticality-score',
        provenance: 'lf-criticality-score',
      }),
    )
    await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Old name',
      classificationKind: 'non_lf_new_project',
      proposedAction: 'create_segment',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'evaluate',
      skipReason: null,
      reviewReason: null,
    })

    const affected = await promoteProjectCatalogProvenance(qx, 'slack-bot', [
      {
        repoUrl: inserted.repoUrl,
        sourceUrl: 'https://lfx.slack.com/archives/C1/p1',
        organization: 'acme',
        requestText: 'onboard Acme, LF project, https://github.com/acme/',
        projectName: 'Acme',
        requestedBy: 'U1',
      },
    ])

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(affected).toBe(1)
    expect(row).toMatchObject({
      provenance: 'slack-bot',
      sourceUrl: 'https://lfx.slack.com/archives/C1/p1',
      requestHints: { organization: 'acme' },
      requestText: 'onboard Acme, LF project, https://github.com/acme/',
      projectName: 'Acme',
      requestedBy: 'U1',
      classifiedAt: null,
      evaluationAttempts: 0,
    })
  })

  test('keeps the classification of an adopted row that already left evaluate', async ({ qx }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'evaluate',
        source: 'lf-criticality-score',
        provenance: 'lf-criticality-score',
      }),
    )
    const classified = await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Old name',
      classificationKind: 'lf_not_in_pcc',
      proposedAction: 'create_segment',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'review',
      skipReason: null,
      reviewReason: 'needs a look',
    })

    const affected = await promoteProjectCatalogProvenance(qx, 'slack-bot', [
      {
        repoUrl: inserted.repoUrl,
        sourceUrl: 'https://lfx.slack.com/archives/C1/p1',
        organization: 'acme',
        requestText: 'onboard Acme, https://github.com/acme/',
        projectName: 'Acme',
        requestedBy: 'U1',
      },
    ])

    const row = await findProjectCatalogById(qx, inserted.id)
    expect(affected).toBe(1)
    expect(row).toMatchObject({
      provenance: 'slack-bot',
      sourceUrl: 'https://lfx.slack.com/archives/C1/p1',
      requestHints: { organization: 'acme' },
      action: 'review',
      classifiedAt: classified?.classifiedAt,
    })
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

describe('countProjectCatalogOpenBySourceUrl', () => {
  test('counts pending and in-review rows of the same request', async ({ qx }) => {
    const sourceUrl = 'https://slack.test/archives/C1/p1760000000123456'
    await insertProjectCatalog(
      qx,
      catalogRow({ repoUrl: 'https://github.com/a/one', action: 'evaluate', sourceUrl }),
    )
    await insertProjectCatalog(
      qx,
      catalogRow({ repoUrl: 'https://github.com/a/two', action: 'review', sourceUrl }),
    )
    await insertProjectCatalog(
      qx,
      catalogRow({ repoUrl: 'https://github.com/a/three', action: 'skip', sourceUrl }),
    )
    await insertProjectCatalog(
      qx,
      catalogRow({ repoUrl: 'https://github.com/a/five', action: 'error', sourceUrl }),
    )
    await insertProjectCatalog(
      qx,
      catalogRow({
        repoUrl: 'https://github.com/a/four',
        action: 'evaluate',
        sourceUrl: 'https://slack.test/archives/C1/p1760000000999999',
      }),
    )

    await insertProjectCatalog(
      qx,
      catalogRow({ repoUrl: 'https://github.com/a/six', action: 'ask', sourceUrl }),
    )

    expect(await countProjectCatalogOpenBySourceUrl(qx, sourceUrl)).toEqual({
      pending: 1,
      inReview: 1,
      asked: 1,
      failed: 1,
    })
    expect(
      await countProjectCatalogOpenBySourceUrl(qx, 'https://slack.test/archives/C1/p0'),
    ).toEqual({ pending: 0, inReview: 0, asked: 0, failed: 0 })
  })
})

describe('findProjectCatalogByRepoUrls', () => {
  test('returns the matching rows only and short-circuits on an empty input', async ({ qx }) => {
    const one = await insertProjectCatalog(qx, catalogRow({ repoUrl: 'https://github.com/b/one' }))
    const two = await insertProjectCatalog(qx, catalogRow({ repoUrl: 'https://github.com/b/two' }))
    await insertProjectCatalog(qx, catalogRow({ repoUrl: 'https://github.com/b/three' }))

    const rows = await findProjectCatalogByRepoUrls(qx, [
      'https://github.com/b/one',
      'https://github.com/b/two',
      'https://github.com/b/missing',
    ])

    expect(rows.map((row) => row.id).sort()).toEqual([one.id, two.id].sort())
    expect(await findProjectCatalogByRepoUrls(qx, [])).toEqual([])
  })
})

describe('isProjectCatalogOnboardingPending', () => {
  const now = Date.parse('2026-10-19T12:00:00Z')

  test('counts unclaimed and stale-claimed onboard rows, nothing else', () => {
    expect(isProjectCatalogOnboardingPending({ action: 'onboard', onboardedAt: null }, now)).toBe(
      true,
    )
    expect(
      isProjectCatalogOnboardingPending(
        { action: 'onboard', onboardedAt: '2026-10-19T11:00:00Z' },
        now,
      ),
    ).toBe(true)
    expect(
      isProjectCatalogOnboardingPending(
        { action: 'onboard', onboardedAt: '2026-10-19T11:50:00Z' },
        now,
      ),
    ).toBe(false)
    expect(isProjectCatalogOnboardingPending({ action: 'onboarded', onboardedAt: null }, now)).toBe(
      false,
    )
  })
})

describe('reopenRejectedProjectCatalog', () => {
  const reopenRequest = {
    repoUrl: 'https://github.com/gerritcodereview/gerrit',
    sourceUrl: 'https://slack.test/archives/C1/p2',
    requestText: 'onboard Gerrit https://github.com/gerritcodereview/gerrit',
    projectName: 'Gerrit',
    requestedBy: 'U2',
    organization: 'gerritcodereview',
  }

  async function rejectedRow(
    qx: Parameters<typeof insertProjectCatalog>[0],
    decisionReason: string,
  ) {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({
        action: 'evaluate',
        sourceUrl: 'https://slack.test/archives/C1/p1',
        requestText: 'onboard Gerrit project https://github.com/gerritcodereview/gerrit',
        projectName: 'Gerrit project',
      }),
    )
    await markProjectCatalogClassified(qx, inserted.id, {
      projectName: 'Gerrit project',
      classificationKind: 'ambiguous',
      proposedAction: 'human_review',
      pccProjectId: null,
      matchedProjectName: null,
      matchedSegmentId: null,
      action: 'review',
      skipReason: null,
      reviewReason: 'no match',
    })
    await setProjectCatalogReviewMessageTs(qx, inserted.id, '1700000000.000100')
    await resolveProjectCatalogReview(qx, inserted.id, {
      decision: 'reject',
      reviewedBy: 'U1',
      decisionReason,
      decisionNote: 'typo',
    })
    return inserted
  }

  test('reopens a row rejected for a requester error and returns the earlier thread and reason', async ({
    qx,
  }) => {
    const inserted = await rejectedRow(qx, 'other')

    const reopened = await reopenRejectedProjectCatalog(
      qx,
      [reopenRequest],
      ['wrong_project', 'other'],
    )

    expect(reopened).toEqual([
      {
        id: inserted.id,
        repoUrl: inserted.repoUrl,
        previousSourceUrl: 'https://slack.test/archives/C1/p1',
        previousDecisionReason: 'other',
      },
    ])
    const row = await findProjectCatalogById(qx, inserted.id)
    expect(row).toMatchObject({
      action: 'auto',
      source: 'slack-bot',
      provenance: 'slack-bot',
      sourceUrl: 'https://slack.test/archives/C1/p2',
      requestText: reopenRequest.requestText,
      projectName: 'Gerrit',
      requestedBy: 'U2',
      requestHints: { organization: 'gerritcodereview' },
      skipReason: null,
      classificationKind: null,
      proposedAction: null,
      classifiedAt: null,
      reviewedAt: null,
      reviewedBy: null,
      reviewMessageTs: null,
      reviewReason: null,
      decisionReason: null,
      decisionNote: null,
      evaluationAttempts: 0,
    })
  })

  test('leaves a row rejected for another reason untouched', async ({ qx }) => {
    const inserted = await rejectedRow(qx, 'not_lf_project')

    const reopened = await reopenRejectedProjectCatalog(
      qx,
      [reopenRequest],
      ['wrong_project', 'other'],
    )

    expect(reopened).toEqual([])
    expect(await findProjectCatalogById(qx, inserted.id)).toMatchObject({
      action: 'skip',
      reviewedBy: 'U1',
      decisionReason: 'not_lf_project',
      sourceUrl: 'https://slack.test/archives/C1/p1',
    })
  })

  test('clears the organization hint and the requester when the new request has none', async ({
    qx,
  }) => {
    const inserted = await rejectedRow(qx, 'wrong_project')

    const reopened = await reopenRejectedProjectCatalog(
      qx,
      [{ ...reopenRequest, requestedBy: null, organization: null }],
      ['wrong_project', 'other'],
    )

    expect(reopened).toHaveLength(1)
    expect(await findProjectCatalogById(qx, inserted.id)).toMatchObject({
      action: 'auto',
      requestedBy: null,
      requestHints: null,
    })
  })

  test('leaves a row skipped without a review untouched', async ({ qx }) => {
    const inserted = await insertProjectCatalog(
      qx,
      catalogRow({ action: 'skip', skipReason: 'repository already tracked in CDP' }),
    )

    const reopened = await reopenRejectedProjectCatalog(
      qx,
      [reopenRequest],
      ['wrong_project', 'other'],
    )

    expect(reopened).toEqual([])
    expect((await findProjectCatalogById(qx, inserted.id))?.action).toBe('skip')
  })
})
