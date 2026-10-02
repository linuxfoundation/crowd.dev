import { describe, expect, it, vi } from 'vitest'

import { IDbProjectCatalogCreate } from '@crowd/data-access-layer/src/project-catalog/types'
import { IPccCandidate } from '@crowd/project-onboarding'

import {
  AMBIGUOUS_SKIP_REASON,
  IRequestClassificationDeps,
  LF_NOT_IN_CDP_SKIP_REASON,
  classifyDiscussionRows,
  classifyDiscussions,
} from './requestClassification'

const SOURCE_URL = 'https://github.com/linuxfoundation/insights/discussions/1'

function row(overrides: Partial<IDbProjectCatalogCreate> = {}): IDbProjectCatalogCreate {
  return {
    projectSlug: 'acme-one',
    repoName: 'one',
    repoUrl: 'https://github.com/acme/one',
    source: 'insights-discussions',
    sourceUrl: SOURCE_URL,
    provenance: 'github-discussion',
    action: 'auto',
    ...overrides,
  } as IDbProjectCatalogCreate
}

function llmAnswer(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    repoUrls: ['https://github.com/acme/one'],
    linkUrls: [],
    projectName: 'Acme',
    declaredLf: null,
    asksAboutHierarchy: false,
    ...overrides,
  })
}

function pccCandidate(overrides: Partial<IPccCandidate> = {}): IPccCandidate {
  return { projectId: 'pcc-1', name: 'Acme', slug: 'acme', score: 1, isLeaf: true, ...overrides }
}

function deps(
  answer: string | null,
  candidates: IPccCandidate[] | undefined,
): IRequestClassificationDeps {
  return {
    queryLlm: vi.fn().mockResolvedValue(answer),
    lookups: {
      findPccCandidates: candidates ? vi.fn().mockResolvedValue(candidates) : undefined,
      findCdpSegmentByPccProject: vi.fn().mockResolvedValue(null),
    },
  }
}

const REQUEST_TEXT = 'Please onboard Acme https://github.com/acme/one'

describe('classifyDiscussionRows', () => {
  it('keeps the rows untouched and sends no alert for a new non-LF project', async () => {
    const rows = [row()]

    const result = await classifyDiscussionRows(rows, REQUEST_TEXT, deps(llmAnswer(), []))

    expect(result).toEqual({ rows, alerts: [] })
  })

  it('skips the rows and alerts when the project is an LF project not yet in CDP', async () => {
    const pccProject = pccCandidate()

    const result = await classifyDiscussionRows(
      [row()],
      REQUEST_TEXT,
      deps(llmAnswer(), [pccProject]),
    )

    expect(result.rows).toEqual([
      { ...row(), action: 'skip', skipReason: LF_NOT_IN_CDP_SKIP_REASON },
    ])
    expect(result.alerts).toEqual([
      {
        sourceUrl: SOURCE_URL,
        repoUrls: ['https://github.com/acme/one'],
        resolution: { kind: 'lf_not_in_cdp', pccProject },
      },
    ])
  })

  it('skips with an ambiguous alert when Snowflake is not configured', async () => {
    const result = await classifyDiscussionRows([row()], REQUEST_TEXT, deps(llmAnswer(), undefined))

    expect(result.rows[0]).toMatchObject({ action: 'skip' })
    expect(result.rows[0].skipReason).toContain(AMBIGUOUS_SKIP_REASON)
    expect(result.alerts[0].resolution).toMatchObject({
      kind: 'ambiguous',
      reason: 'PCC lookup is not configured',
    })
  })

  it('skips with an ambiguous alert when the LLM does not answer', async () => {
    const result = await classifyDiscussionRows([row()], REQUEST_TEXT, deps(null, []))

    expect(result.rows[0]).toMatchObject({ action: 'skip' })
    expect(result.alerts[0].resolution.kind).toBe('ambiguous')
  })

  it('skips with an ambiguous alert instead of failing when a lookup throws', async () => {
    const failing = deps(llmAnswer(), [])
    failing.lookups.findPccCandidates = vi.fn().mockRejectedValue(new Error('warehouse down'))

    const result = await classifyDiscussionRows([row()], REQUEST_TEXT, failing)

    expect(result.rows[0]).toMatchObject({ action: 'skip' })
    expect(result.alerts[0].resolution).toMatchObject({
      kind: 'ambiguous',
      reason: 'Classification failed: warehouse down',
    })
  })
})

describe('classifyDiscussions', () => {
  it('classifies each discussion once and keeps the row order', async () => {
    const second = row({
      repoUrl: 'https://github.com/other/two',
      sourceUrl: 'https://github.com/linuxfoundation/insights/discussions/2',
    })
    const rows = [row(), second, row({ repoUrl: 'https://github.com/acme/three' })]
    const texts = new Map([
      [SOURCE_URL, REQUEST_TEXT],
      [second.sourceUrl as string, 'Also https://github.com/acme/one'],
    ])
    const classificationDeps = deps(llmAnswer(), [])

    const result = await classifyDiscussions(rows, texts, classificationDeps)

    expect(result.rows).toEqual(rows)
    expect(result.alerts).toEqual([])
    expect(classificationDeps.queryLlm).toHaveBeenCalledTimes(2)
  })

  it('leaves rows without a request text untouched', async () => {
    const rows = [row({ sourceUrl: null })]
    const classificationDeps = deps(llmAnswer(), [pccCandidate()])

    const result = await classifyDiscussions(rows, new Map(), classificationDeps)

    expect(result).toEqual({ rows, alerts: [] })
    expect(classificationDeps.queryLlm).not.toHaveBeenCalled()
  })

  it('skips every row of a discussion that resolves to an alert', async () => {
    const rows = [row(), row({ repoUrl: 'https://github.com/acme/two' })]

    const result = await classifyDiscussions(
      rows,
      new Map([[SOURCE_URL, REQUEST_TEXT]]),
      deps(llmAnswer(), [pccCandidate()]),
    )

    expect(result.rows.map((r) => r.action)).toEqual(['skip', 'skip'])
    expect(result.alerts).toHaveLength(1)
    expect(result.alerts[0].repoUrls).toEqual([
      'https://github.com/acme/one',
      'https://github.com/acme/two',
    ])
  })
})
