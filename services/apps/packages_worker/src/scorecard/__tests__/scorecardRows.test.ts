import { describe, expect, it } from 'vitest'

import { scorecardProjectPath, toScorecardRows } from '../scorecardRows'

describe('scorecardProjectPath', () => {
  it('preserves the owner and repo case', () => {
    expect(scorecardProjectPath('https://github.com/chartjs/Chart.js')).toBe(
      'github.com/chartjs/Chart.js',
    )
  })

  it('strips .git and trailing slashes', () => {
    expect(scorecardProjectPath('https://github.com/Owner/Repo.git')).toBe('github.com/Owner/Repo')
    expect(scorecardProjectPath('https://github.com/Owner/Repo/')).toBe('github.com/Owner/Repo')
  })

  it('returns null for non-GitHub hosts', () => {
    expect(scorecardProjectPath('https://gitlab.com/owner/repo')).toBeNull()
  })

  it('returns null without an owner and repo pair', () => {
    expect(scorecardProjectPath('https://github.com/owner')).toBeNull()
    expect(scorecardProjectPath('https://github.com/owner/repo/tree/main')).toBeNull()
    expect(scorecardProjectPath('not-a-url')).toBeNull()
  })
})

describe('toScorecardRows', () => {
  const payload = {
    date: '2026-10-19',
    score: 7.64,
    checks: [
      { name: 'Code-Review', score: 10, reason: 'all changesets reviewed' },
      { name: 'Fuzzing', score: -1, reason: null },
      { name: 'License', score: 9, reason: 'license file\u0000detected' },
    ],
  }

  it('maps the score, the scan date and every check', () => {
    const rows = toScorecardRows(42, payload)
    expect(rows?.repo).toEqual({ repoId: 42, score: 7.6, lastRunAt: new Date('2026-10-19') })
    expect(rows?.checks).toEqual([
      { repoId: 42, checkName: 'Code-Review', score: 10, reason: 'all changesets reviewed' },
      { repoId: 42, checkName: 'Fuzzing', score: null, reason: '' },
      { repoId: 42, checkName: 'License', score: 9, reason: 'license filedetected' },
    ])
  })

  it('accepts the datetime form of date used by action-published results', () => {
    const rows = toScorecardRows(1, { ...payload, date: '2026-11-02T05:10:09Z' })
    expect(rows?.repo.lastRunAt).toEqual(new Date('2026-11-02T05:10:09Z'))
  })

  it('keeps the last occurrence of a duplicated check name', () => {
    const rows = toScorecardRows(1, {
      ...payload,
      checks: [
        { name: 'License', score: 5, reason: 'old' },
        { name: 'License', score: 9, reason: 'new' },
      ],
    })
    expect(rows?.checks).toEqual([{ repoId: 1, checkName: 'License', score: 9, reason: 'new' }])
  })

  it('returns null for an unparseable date', () => {
    expect(toScorecardRows(1, { ...payload, date: 'yesterday' })).toBeNull()
  })
})
