import { readFileSync } from 'fs'
import { join } from 'path'
import { describe, expect, test } from 'vitest'

import { computeScores } from './computeScores'
import type { ICheckResult } from './types'

function loadFixture(name: string): ICheckResult[] {
  return JSON.parse(readFileSync(join(__dirname, `__fixtures__/${name}-results.json`), 'utf-8'))
}

function check(category: string, status: ICheckResult['status'], id = category): ICheckResult {
  return { id, category, status, message: '' }
}

describe('computeScores', () => {
  test('a category score is passed / applicable, skip excluded from both sides', () => {
    const { categoryScores } = computeScores([
      check('content-discoverability', 'pass'),
      check('content-discoverability', 'pass'),
      check('content-discoverability', 'fail'),
      check('content-discoverability', 'skip'),
    ])
    // 2 passed / 3 applicable (skip excluded) = 66.67 -> 67
    expect(categoryScores['content-discoverability']).toBe(67)
  })

  test('warn does not earn partial credit — it counts as not passed', () => {
    const { categoryScores } = computeScores([
      check('observability', 'pass'),
      check('observability', 'warn'),
    ])
    expect(categoryScores.observability).toBe(50)
  })

  test('error counts as not passed, same as fail', () => {
    const { categoryScores } = computeScores([
      check('authentication', 'pass'),
      check('authentication', 'error'),
    ])
    expect(categoryScores.authentication).toBe(50)
  })

  test('an all-skipped category gets full credit (100)', () => {
    const { categoryScores } = computeScores([
      check('url-stability', 'skip'),
      check('url-stability', 'skip'),
    ])
    expect(categoryScores['url-stability']).toBe(100)
  })

  test('a category with zero results (never appeared in the report) also gets full credit', () => {
    const { categoryScores } = computeScores([check('authentication', 'pass')])
    expect(categoryScores['content-discoverability']).toBe(100)
  })

  test('overall score is the weighted average across all 7 categories, weights 30/15/15/10/10/10/10', () => {
    const allPassExcept = (failing: string) =>
      Object.keys({
        'content-discoverability': 0,
        'markdown-availability': 0,
        'page-size': 0,
        'content-structure': 0,
        'url-stability': 0,
        observability: 0,
        authentication: 0,
      }).map((category) => check(category, category === failing ? 'fail' : 'pass'))

    // Failing only content-discoverability (weight 30) should drop the overall score by
    // roughly 30 points from a perfect 100 (0 * 30 + 100 * 70) / 100 = 70.
    const { overallScore } = computeScores(allPassExcept('content-discoverability'))
    expect(overallScore).toBe(70)

    // Failing only authentication (weight 10) should drop it by roughly 10.
    const { overallScore: overallScore2 } = computeScores(allPassExcept('authentication'))
    expect(overallScore2).toBe(90)
  })

  test.each([
    [100, 'A'],
    [90, 'A'],
    [89, 'B'],
    [80, 'B'],
    [79, 'C'],
    [70, 'C'],
    [69, 'D'],
    [60, 'D'],
    [59, 'F'],
    [0, 'F'],
  ])('a score of %i maps to grade %s', (score, grade) => {
    // Give every category the same pass ratio, so the weighted average across all 7
    // categories equals that same ratio regardless of the weight distribution — isolating
    // the grade boundary from the weighting logic already covered above.
    const categories = [
      'content-discoverability',
      'markdown-availability',
      'page-size',
      'content-structure',
      'url-stability',
      'observability',
      'authentication',
    ]
    const results: ICheckResult[] = categories.flatMap((category) =>
      Array.from({ length: 100 }, (_, i) =>
        check(category, i < score ? 'pass' : 'fail', `${category}-${i}`),
      ),
    )

    const { overallScore, overallGrade } = computeScores(results)
    expect(overallScore).toBe(score)
    expect(overallGrade).toBe(grade)
  })
})

describe('computeScores — real afdocs reports (kyverno, openfga)', () => {
  test('kyverno: mostly-failing docs site scores F with hand-verified category breakdown', () => {
    const { overallScore, overallGrade, categoryScores } = computeScores(loadFixture('kyverno'))

    expect(categoryScores).toEqual({
      'content-discoverability': 0,
      'markdown-availability': 0,
      'page-size': 67,
      'content-structure': 100,
      'url-stability': 100,
      observability: 100,
      authentication: 100,
    })
    expect(overallScore).toBe(50)
    expect(overallGrade).toBe('F')
  })

  test('openfga: a mixed report (including a real warn) scores D with hand-verified category breakdown', () => {
    const { overallScore, overallGrade, categoryScores } = computeScores(loadFixture('openfga'))

    expect(categoryScores).toEqual({
      'content-discoverability': 71,
      'markdown-availability': 50,
      'page-size': 25,
      'content-structure': 33,
      'url-stability': 100,
      observability: 67,
      authentication: 100,
    })
    expect(overallScore).toBe(63)
    expect(overallGrade).toBe('D')
  })
})
