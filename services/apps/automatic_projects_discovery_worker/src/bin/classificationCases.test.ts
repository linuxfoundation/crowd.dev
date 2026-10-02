import { describe, expect, it } from 'vitest'

import { CLASSIFICATION_CASES } from './classificationCases'

describe('CLASSIFICATION_CASES', () => {
  it('has unique ids', () => {
    const ids = CLASSIFICATION_CASES.map((testCase) => testCase.id)

    expect(new Set(ids).size).toBe(ids.length)
  })

  it.each(CLASSIFICATION_CASES.map((testCase) => [testCase.id, testCase] as const))(
    '%s names every repository in its text and expects at least one node',
    (_id, testCase) => {
      testCase.repoUrls.forEach((repoUrl) => expect(testCase.text).toContain(repoUrl))
      expect(testCase.expectedNodes.length).toBeGreaterThan(0)
    },
  )
})
