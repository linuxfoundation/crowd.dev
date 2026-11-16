import { describe, expect, it, vi } from 'vitest'

vi.mock('@crowd/data-access-layer', () => ({ findProjectCatalogBySourceUrl: vi.fn() }))

import { findProjectCatalogBySourceUrl } from '@crowd/data-access-layer'
import { IDbProjectCatalog } from '@crowd/data-access-layer/src/project-catalog/types'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import {
  findRequestGroupSiblings,
  hasManyRepositories,
  organizationOf,
  organizationSubject,
  requestGroupKey,
  requestGroupOf,
  requestGroupReview,
  requestGroupReviewOf,
  requestGroupSiblings,
  sameQuestion,
  sameReviewProposal,
} from './organizationGroup'

const source = 'https://lfx.slack.com/archives/C1/p1'
const repoUrl = 'https://github.com/openbmc/bmcweb'
const rows = [
  { id: 'a', sourceUrl: source, repoUrl, requestHints: { organization: 'openbmc' } },
  { id: 'b', sourceUrl: source, repoUrl, requestHints: { organization: 'openbmc' } },
  { id: 'c', sourceUrl: source, repoUrl, requestHints: null },
  {
    id: 'd',
    sourceUrl: 'https://lfx.slack.com/archives/C1/p2',
    repoUrl,
    requestHints: { organization: 'openbmc' },
  },
  { id: 'e', sourceUrl: source, repoUrl, requestHints: { organization: 'other' } },
]
const project = (id: string, repoUrl: string, name = 'Apache HugeGraph') => ({
  id,
  sourceUrl: source,
  repoUrl,
  requestHints: { project: name, mainRepoUrl: 'https://github.com/apache/hugegraph' },
})
const projectRows = [
  project('p1', 'https://github.com/apache/hugegraph'),
  project('p2', 'https://github.com/apache/hugegraph-ai'),
  project('p3', 'https://github.com/apache/hugegraph-doc', 'apache hugegraph'),
  project('p4', 'https://github.com/other/thing', 'Other'),
]

describe('organization groups', () => {
  it('groups the rows of one organization request by origin and owner', () => {
    expect(organizationOf(rows[0])).toBe('openbmc')
    expect(organizationOf(rows[2])).toBeNull()
    expect(requestGroupOf(rows[0])).toEqual({ kind: 'organization', owner: 'openbmc' })
    expect(requestGroupOf(rows[2])).toBeNull()
    expect(requestGroupKey(rows[0])).toBe(`${source}|org:openbmc`)
    expect(requestGroupKey(rows[2])).toBeNull()
    expect(requestGroupSiblings(rows, rows[0]).map((row) => row.id)).toEqual(['b'])
    expect(requestGroupSiblings(rows, rows[2])).toEqual([])
  })

  it('groups the listed repositories of one request by origin and project name', () => {
    expect(requestGroupOf(projectRows[0])).toEqual({ kind: 'project', name: 'Apache HugeGraph' })
    expect(requestGroupKey(projectRows[0])).toBe(`${source}|project:apache hugegraph`)
    expect(requestGroupKey({ ...projectRows[0], sourceUrl: null })).toBeNull()
    expect(requestGroupOf({ requestHints: { project: '  ' } })).toBeNull()
    expect(requestGroupSiblings(projectRows, projectRows[0]).map((row) => row.id)).toEqual([
      'p2',
      'p3',
    ])
    expect(requestGroupOf({ requestHints: { organization: 'acme', project: 'Acme' } })).toEqual({
      kind: 'organization',
      owner: 'acme',
    })
  })

  it('describes the group under review from the lead and its siblings', () => {
    expect(requestGroupReviewOf(rows[0], 11)).toEqual({ owner: 'openbmc', repositories: 12 })
    expect(requestGroupReviewOf(rows[0], 0)).toBeNull()
    expect(requestGroupReviewOf(rows[2], 3)).toBeNull()
    expect(requestGroupReviewOf(projectRows[0], 4)).toEqual({
      owner: 'apache',
      name: 'Apache HugeGraph',
      repositories: 5,
    })
  })

  it('lists the repositories and the confidence of a project group under review', () => {
    const lead = {
      ...projectRows[0],
      requestHints: { ...projectRows[0].requestHints, confidence: 0.9 },
    }
    expect(requestGroupReview(lead, [projectRows[1], projectRows[2]])).toEqual({
      owner: 'apache',
      name: 'Apache HugeGraph',
      repositories: 3,
      repoUrls: [
        'https://github.com/apache/hugegraph',
        'https://github.com/apache/hugegraph-ai',
        'https://github.com/apache/hugegraph-doc',
      ],
      confidence: 0.9,
    })
    expect(requestGroupReview(projectRows[0], [projectRows[1]])).toMatchObject({ confidence: null })
    expect(requestGroupReview(rows[0], [rows[1]])).toEqual({
      owner: 'openbmc',
      repositories: 2,
      repoUrls: [rows[0].repoUrl, rows[1].repoUrl],
    })
    expect(requestGroupReview(rows[0], [])).toBeNull()
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
    expect(
      organizationSubject({ owner: 'apache', name: 'Apache <HugeGraph>', repositories: 5 }),
    ).toBe('*Apache &lt;HugeGraph&gt;* (5 repositories)')
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

    const siblings = await findRequestGroupSiblings(qx, catalog[0], (row) => row.id !== 'b')
    expect(siblings).toEqual([])
    expect(await findRequestGroupSiblings(qx, catalog[0], () => true)).toEqual([catalog[1]])
    expect(findProjectCatalogBySourceUrl).toHaveBeenCalledWith(qx, source)

    vi.mocked(findProjectCatalogBySourceUrl).mockClear()
    expect(await findRequestGroupSiblings(qx, catalog[2], () => true)).toEqual([])
    expect(findProjectCatalogBySourceUrl).not.toHaveBeenCalled()
  })
})
