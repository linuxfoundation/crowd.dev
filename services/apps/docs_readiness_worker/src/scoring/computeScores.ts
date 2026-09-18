import type { ICheckResult } from './types'

export const CATEGORY_WEIGHTS: Record<string, number> = {
  'content-discoverability': 30,
  'markdown-availability': 15,
  'page-size': 15,
  'content-structure': 10,
  'url-stability': 10,
  observability: 10,
  authentication: 10,
}

export interface IComputedScores {
  overallScore: number
  overallGrade: string
  categoryScores: Record<string, number>
}

function scoreToGrade(score: number): string {
  if (score >= 90) return 'A'
  if (score >= 80) return 'B'
  if (score >= 70) return 'C'
  if (score >= 60) return 'D'
  return 'F'
}

export function computeScores(results: ICheckResult[]): IComputedScores {
  const byCategory: Record<string, { passed: number; applicable: number }> = {}

  for (const result of results) {
    if (result.status === 'skip') {
      continue
    }
    const bucket = (byCategory[result.category] ??= { passed: 0, applicable: 0 })
    bucket.applicable += 1
    if (result.status === 'pass') {
      bucket.passed += 1
    }
  }

  const categoryScores: Record<string, number> = {}
  let weightedSum = 0
  let totalWeight = 0

  for (const [category, weight] of Object.entries(CATEGORY_WEIGHTS)) {
    const bucket = byCategory[category]
    const score =
      !bucket || bucket.applicable === 0
        ? 100
        : Math.round((bucket.passed / bucket.applicable) * 100)
    categoryScores[category] = score
    weightedSum += score * weight
    totalWeight += weight
  }

  const overallScore = totalWeight > 0 ? Math.round(weightedSum / totalWeight) : 0

  return { overallScore, overallGrade: scoreToGrade(overallScore), categoryScores }
}
