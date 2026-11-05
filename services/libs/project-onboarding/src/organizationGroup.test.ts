import { describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/data-access-layer', () => ({ findProjectCatalogBySourceUrl: vi.fn() }))

import { findProjectCatalogBySourceUrl } from '@crowd/data-access-layer'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import {
  findOrganizationSiblings,
  hasManyRepositories,
  organizationGroupKey,
  organizationOf,
  organizationReviewOf,
  organizationSiblings,
  organizationSubject,
  sameQuestion,
  sameReviewProposal,
} from './organizationGroup'

const source = 'https://lfx.slack.com/archives/C1/p1'
const rows = [
  { id: 'a', sourceUrl: source, requestHints: { organization: 'openbmc' } },
  { id: 'b', sourceUrl: source, requestHints: { organization: 'openbmc' } },
  { id: 'c', sourceUrl: source, requestHints: null },
  {
    id: 'd',
    sourceUrl: 'https://lfx.slack.com/archives/C1/p2',
    requestHints: { organization: 'openbmc' },
  },
  { id: 'e', sourceUrl: source, requestHints: { organization: 'other' } },
]

describe('organization groups', () => {
  it('groups the rows of one organization request by origin and owner', () => {
    expect(organizationOf(rows[0])).toBe('openbmc')
    expect(organizationOf(rows[2])).toBeNull()
    expect(organizationGroupKey(rows[0])).toBe(`${source}|openbmc`)
    expect(organizationGroupKey(rows[2])).toBeNull()
    expect(organizationSiblings(rows, rows[0]).map((row) => row.id)).toEqual(['b'])
    expect(organizationSiblings(rows, rows[2])).toEqual([])
  })

  it('describes the organization under review from the lead and its siblings', () => {
    expect(organizationReviewOf(rows[0], 11)).toEqual({ owner: 'openbmc', repositories: 12 })
    expect(organizationReviewOf(rows[0], 0)).toBeNull()
    expect(organizationReviewOf(rows[2], 3)).toBeNull()
  })

  it('only folds siblings that carry the same proposal or the same question', () => {
    const lead = {
      classificationKind: 'lf_in_cdp' as const,
      proposedAction: 'update_integration' as const,
      pccProjectId: 'pcc-1',
      matchedSegmentId: 'seg-1',
    }
    expect(sameReviewProposal({ ...lead }, lead)).toBe(true)
    expect(sameReviewProposal({ ...lead, classificationKind: 'lf_not_in_cdp' }, lead)).toBe(false)
    expect(sameReviewProposal({ ...lead, proposedAction: 'human_review' }, lead)).toBe(false)
    expect(sameReviewProposal({ ...lead, matchedSegmentId: 'seg-2' }, lead)).toBe(false)
    expect(sameReviewProposal({ ...lead, pccProjectId: 'pcc-2' }, lead)).toBe(false)

    const question = {
      kind: 'did_you_mean' as const,
      text: 'Did you mean Acme?',
      options: [
        { id: 'yes', label: 'Yes' },
        { id: 'no', label: 'No' },
      ],
    }
    expect(sameQuestion({ ...question, messageTs: '1' }, question)).toBe(true)
    expect(sameQuestion({ ...question, text: 'Did you mean Acme Labs?' }, question)).toBe(false)
    expect(sameQuestion({ ...question, options: [question.options[0]] }, question)).toBe(false)
    expect(sameQuestion(null, question)).toBe(false)
  })

  it('names the organization with the right number of repositories', () => {
    expect(organizationSubject({ owner: 'a&b', repositories: 12 })).toBe(
      '*github.com/a&amp;b* (12 repositories)',
    )
    expect(organizationSubject({ owner: 'acme', repositories: 1 })).toBe(
      '*github.com/acme* (1 repository)',
    )
    expect(hasManyRepositories({ owner: 'acme', repositories: 1 })).toBe(false)
    expect(hasManyRepositories({ owner: 'acme', repositories: 2 })).toBe(true)
    expect(hasManyRepositories(null)).toBe(false)
  })

  it('loads the siblings of a lead from its thread and keeps the ones that match', async () => {
    const qx = {} as QueryExecutor
    const catalog = rows.map((row) => ({ ...row, action: 'review' })) as IDbProjectCatalog[]
    vi.mocked(findProjectCatalogBySourceUrl).mockResolvedValue(catalog)

    const siblings = await findOrganizationSiblings(qx, catalog[0], (row) => row.id !== 'b')
    expect(siblings).toEqual([])
    expect(await findOrganizationSiblings(qx, catalog[0], () => true)).toEqual([catalog[1]])
    expect(findProjectCatalogBySourceUrl).toHaveBeenCalledWith(qx, source)

    vi.mocked(findProjectCatalogBySourceUrl).mockClear()
    expect(await findOrganizationSiblings(qx, catalog[2], () => true)).toEqual([])
    expect(findProjectCatalogBySourceUrl).not.toHaveBeenCalled()
  })
})
