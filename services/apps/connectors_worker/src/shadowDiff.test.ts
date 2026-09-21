import { describe, expect, it } from 'vitest'

import { IDiffableRecord, diffShadowAgainstNango } from './shadowDiff'

describe('diffShadowAgainstNango', () => {
  it('returns no mismatches when records match on all pass-through fields', () => {
    const shadow: IDiffableRecord[] = [
      {
        sourceId: 'issue-1',
        type: 'issues-comment',
        data: {
          type: 'issues-comment',
          sourceId: 'issue-1',
          sourceParentId: 'issue-parent',
          score: 1,
          title: 'title',
          body: 'body',
          url: 'https://example.com',
          attributes: { foo: 'bar' },
          timestamp: '2026-09-10T00:00:00.000Z',
          memberId: 'should-be-ignored',
        },
      },
    ]
    const nango: IDiffableRecord[] = [
      {
        sourceId: 'issue-1',
        type: 'issues-comment',
        data: {
          type: 'issues-comment',
          sourceId: 'issue-1',
          sourceParentId: 'issue-parent',
          score: 1,
          title: 'title',
          body: 'body',
          url: 'https://example.com',
          attributes: { foo: 'bar' },
          timestamp: '2026-09-10T00:00:00.000Z',
          memberId: 'different-and-fine',
        },
      },
    ]

    expect(diffShadowAgainstNango(shadow, nango)).toEqual([])
  })

  it('reports a field_mismatch with the differing field when a pass-through field diverges', () => {
    const shadow: IDiffableRecord[] = [
      { sourceId: 'issue-1', type: 'issues-comment', data: { body: 'old body' } },
    ]
    const nango: IDiffableRecord[] = [
      { sourceId: 'issue-1', type: 'issues-comment', data: { body: 'new body' } },
    ]

    const result = diffShadowAgainstNango(shadow, nango)

    expect(result).toEqual([
      {
        sourceId: 'issue-1',
        type: 'issues-comment',
        kind: 'field_mismatch',
        severity: 'high',
        fields: [{ field: 'body', shadowValue: 'old body', nangoValue: 'new body' }],
      },
    ])
  })

  it('truncates large field values so a single mismatch cannot blow up the activity payload', () => {
    const hugeShadowBody = 'a'.repeat(1000)
    const hugeNangoBody = 'b'.repeat(1000)
    const shadow: IDiffableRecord[] = [
      { sourceId: 'issue-1', type: 'issues-comment', data: { body: hugeShadowBody } },
    ]
    const nango: IDiffableRecord[] = [
      { sourceId: 'issue-1', type: 'issues-comment', data: { body: hugeNangoBody } },
    ]

    const result = diffShadowAgainstNango(shadow, nango)

    expect(result).toHaveLength(1)
    const field = result[0].fields?.[0]
    expect(field?.field).toBe('body')
    expect((field?.shadowValue as string | undefined)?.length).toBeLessThan(hugeShadowBody.length)
    expect(field?.shadowValue).toContain('[truncated]')
    expect((field?.nangoValue as string | undefined)?.length).toBeLessThan(hugeNangoBody.length)
    expect(field?.nangoValue).toContain('[truncated]')
  })

  it('reports high-severity missing_in_nango when a shadow record has no nango counterpart', () => {
    const shadow: IDiffableRecord[] = [{ sourceId: 'issue-1', type: 'issues-comment', data: {} }]

    const result = diffShadowAgainstNango(shadow, [])

    expect(result).toEqual([
      { sourceId: 'issue-1', type: 'issues-comment', kind: 'missing_in_nango', severity: 'high' },
    ])
  })

  it('reports high-severity missing_in_shadow when a nango record has no shadow counterpart', () => {
    const nango: IDiffableRecord[] = [{ sourceId: 'issue-1', type: 'issues-comment', data: {} }]

    const result = diffShadowAgainstNango([], nango)

    expect(result).toEqual([
      { sourceId: 'issue-1', type: 'issues-comment', kind: 'missing_in_shadow', severity: 'high' },
    ])
  })

  it('keeps records with the same sourceId but different types as distinct entries', () => {
    const shadow: IDiffableRecord[] = [
      { sourceId: 'issue-1', type: 'issue-opened', data: { body: 'opened body' } },
      { sourceId: 'issue-1', type: 'issues-comment', data: { body: 'comment body' } },
    ]
    const nango: IDiffableRecord[] = [
      { sourceId: 'issue-1', type: 'issue-opened', data: { body: 'opened body' } },
    ]

    const result = diffShadowAgainstNango(shadow, nango)

    expect(result).toEqual([
      { sourceId: 'issue-1', type: 'issues-comment', kind: 'missing_in_nango', severity: 'high' },
    ])
  })

  it('ignores additions/deletions/changedFiles drift on pull request attributes', () => {
    const shadow: IDiffableRecord[] = [
      {
        sourceId: 'pr-1',
        type: 'pull_request-opened',
        data: { attributes: { additions: 10, deletions: 2, changedFiles: 3, state: 'open' } },
      },
    ]
    const nango: IDiffableRecord[] = [
      {
        sourceId: 'pr-1',
        type: 'pull_request-opened',
        data: { attributes: { additions: 15, deletions: 4, changedFiles: 5, state: 'open' } },
      },
    ]

    expect(diffShadowAgainstNango(shadow, nango)).toEqual([])
  })

  it('still reports a mismatch on pull request attributes outside the snapshot fields', () => {
    const shadow: IDiffableRecord[] = [
      {
        sourceId: 'pr-1',
        type: 'pull_request-opened',
        data: { attributes: { additions: 10, state: 'open' } },
      },
    ]
    const nango: IDiffableRecord[] = [
      {
        sourceId: 'pr-1',
        type: 'pull_request-opened',
        data: { attributes: { additions: 15, state: 'closed' } },
      },
    ]

    const result = diffShadowAgainstNango(shadow, nango)

    expect(result).toEqual([
      {
        sourceId: 'pr-1',
        type: 'pull_request-opened',
        kind: 'field_mismatch',
        severity: 'high',
        fields: [
          { field: 'attributes', shadowValue: { state: 'open' }, nangoValue: { state: 'closed' } },
        ],
      },
    ])
  })

  it('downgrades pull_request-review-requested one-sided mismatches to low severity', () => {
    const shadow: IDiffableRecord[] = [
      { sourceId: 'pr-1-reviewer-a', type: 'pull_request-review-requested', data: {} },
      { sourceId: 'pr-1-reviewer-b', type: 'pull_request-review-requested', data: {} },
    ]
    const nango: IDiffableRecord[] = [
      { sourceId: 'pr-1-aggregated', type: 'pull_request-review-requested', data: {} },
    ]

    const result = diffShadowAgainstNango(shadow, nango)

    expect(result).toHaveLength(3)
    expect(result.every((m) => m.severity === 'low')).toBe(true)
  })
})
