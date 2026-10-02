import { getErrorMessage } from '@crowd/common'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { IDbProjectCatalogCreate } from '@crowd/data-access-layer/src/project-catalog/types'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'
import { withRequestClassifierDeps } from '@crowd/project-onboarding/src/requestClassifierDeps'

import { classifyDiscussionRows } from '../activities/requestClassification'
import { CLASSIFICATION_CASES, IClassificationCase } from './classificationCases'

const log = getServiceLogger()

const SOURCE_URL_PREFIX = 'https://example.invalid/classification-cases'

interface ICaseOutcome {
  id: string
  expected: string
  actual: string
  matches: boolean
  reachableFromDiscussions: boolean
}

function usage(): never {
  log.error('Usage: classify-onboarding-cases [--case <id>] [--list]')
  process.exit(1)
}

function readCaseFilter(argv: string[]): string | undefined {
  const flagIndex = argv.indexOf('--case')
  if (flagIndex === -1) {
    return undefined
  }
  const value = argv[flagIndex + 1]
  if (!value) {
    usage()
  }
  return value
}

function toRow(testCase: IClassificationCase): IDbProjectCatalogCreate[] {
  const repoUrls = testCase.repoUrls.length > 0 ? testCase.repoUrls : ['']

  return repoUrls.map((repoUrl) => ({
    projectSlug: repoUrl.split('/').slice(-2).join('-'),
    repoName: repoUrl.split('/').pop() ?? '',
    repoUrl,
    source: 'insights-discussions',
    sourceUrl: `${SOURCE_URL_PREFIX}/${testCase.id}`,
    provenance: 'github-discussion',
    action: 'auto',
  })) as IDbProjectCatalogCreate[]
}

function selectCases(filter: string | undefined): IClassificationCase[] {
  if (!filter) {
    return CLASSIFICATION_CASES
  }

  const selected = CLASSIFICATION_CASES.filter((testCase) => testCase.id === filter)
  if (selected.length === 0) {
    log.error({ filter, available: CLASSIFICATION_CASES.map((c) => c.id) }, 'Unknown case.')
    process.exit(1)
  }
  return selected
}

function formatOutcomes(outcomes: ICaseOutcome[]): string {
  return outcomes
    .map(
      (outcome) =>
        `${outcome.matches ? 'OK  ' : 'DIFF'} ${outcome.id}\n` +
        `       expected: ${outcome.expected}\n` +
        `       actual:   ${outcome.actual}` +
        (outcome.reachableFromDiscussions
          ? ''
          : '\n       note:     no GitHub repo, never classified by the discussion pipeline today'),
    )
    .join('\n')
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2)
  if (argv.includes('--list')) {
    CLASSIFICATION_CASES.forEach((testCase) =>
      process.stdout.write(`${testCase.id}\t${testCase.description}\n`),
    )
    return
  }

  const cases = selectCases(readCaseFilter(argv))
  const qx = pgpQx(await getDbConnection(WRITE_DB_CONFIG()))

  const outcomes = await withRequestClassifierDeps(qx, async (deps) => {
    const results: ICaseOutcome[] = []
    for (const testCase of cases) {
      const { nodes } = await classifyDiscussionRows(toRow(testCase), testCase.text, deps, true)
      const [actual] = nodes
      results.push({
        id: testCase.id,
        expected: testCase.expectedNodes.join(' | '),
        actual,
        matches: testCase.expectedNodes.includes(actual),
        reachableFromDiscussions: testCase.repoUrls.length > 0,
      })
    }
    return results
  })

  process.stdout.write(`${formatOutcomes(outcomes)}\n`)
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error({ error: getErrorMessage(err) }, 'Classification cases failed.')
    process.exit(1)
  })
