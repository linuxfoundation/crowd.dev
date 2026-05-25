import { IEvaluationInput, IEvaluationResult } from './types'

// TODO: Replace with the actual AI evaluation algorithm once the external repo is integrated.
// The algorithm is described in the technical spec and currently takes ~30-40s per project
// at ~$0.15/project. Reference: https://github.com/... (link TBD).
export async function evaluateProject(input: IEvaluationInput): Promise<IEvaluationResult> {
  console.error(`evaluateProject is not implemented yet for repo: ${input.repoUrl}`)
  return { outcome: 'unsure', reason: 'evaluator not implemented' }
}
