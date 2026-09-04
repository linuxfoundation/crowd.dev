import type { Credential } from '@crowd/connectors'
import { getCredential, mapWithConcurrency } from '@crowd/connectors'
import type { InstallationSummary } from '@crowd/connectors/src/connectors/github/appToken'
import {
  listInstallations,
  mintInstallationToken,
} from '@crowd/connectors/src/connectors/github/appToken'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

const GITHUB_API_VERSION = '2022-11-28'
const REQUEST_TIMEOUT_MS = 30_000
const DEFAULT_CONCURRENCY = 4

interface RateLimitResource {
  limit: number
  remaining: number
  reset: number
}

interface InstallationBudget {
  installation: InstallationSummary
  repoCount: number | null
  graphql: RateLimitResource | null
  core: RateLimitResource | null
  error?: string
}

function usage(): never {
  log.error(
    'Usage: audit-github-app-budget --integration-id <uuid> [--concurrency <n>] [--json] [--include-suspended]',
  )
  process.exit(1)
}

function takeFlag(argv: string[], flag: string): string | undefined {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) {
    return undefined
  }
  const value = argv[flagIndex + 1]
  argv.splice(flagIndex, 2)
  return value
}

function takeSwitch(argv: string[], flag: string): boolean {
  const flagIndex = argv.indexOf(flag)
  if (flagIndex === -1) {
    return false
  }
  argv.splice(flagIndex, 1)
  return true
}

function parseArgs(rawArgv: string[]): {
  integrationId: string
  concurrency: number
  asJson: boolean
  includeSuspended: boolean
} {
  const argv = rawArgv.filter((arg) => arg !== '--')
  const integrationId = takeFlag(argv, '--integration-id')
  const concurrency = Number(takeFlag(argv, '--concurrency') ?? DEFAULT_CONCURRENCY)
  const asJson = takeSwitch(argv, '--json')
  const includeSuspended = takeSwitch(argv, '--include-suspended')
  if (!integrationId || !Number.isInteger(concurrency) || concurrency < 1) {
    usage()
  }
  return { integrationId, concurrency, asJson, includeSuspended }
}

async function githubGet(token: string, url: string): Promise<unknown> {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': GITHUB_API_VERSION,
    },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  })
  const body = await response.json().catch(() => undefined)
  if (!response.ok) {
    const message = (body as { message?: string } | undefined)?.message ?? response.statusText
    throw new Error(`${response.status}: ${message}`)
  }
  return body
}

async function readRepoCount(token: string): Promise<number> {
  const body = await githubGet(token, 'https://api.github.com/installation/repositories?per_page=1')
  return (body as { total_count: number }).total_count
}

async function auditInstallation(
  credential: Credential,
  installation: InstallationSummary,
): Promise<InstallationBudget> {
  try {
    const { token } = await mintInstallationToken(credential, installation.id)
    const [rateLimit, repoCount] = await Promise.all([
      githubGet(token, 'https://api.github.com/rate_limit'),
      readRepoCount(token),
    ])
    const resources = (rateLimit as { resources?: Record<string, RateLimitResource> }).resources
    return {
      installation,
      repoCount,
      graphql: resources?.graphql ?? null,
      core: resources?.core ?? null,
    }
  } catch (err) {
    return {
      installation,
      repoCount: null,
      graphql: null,
      core: null,
      error: err instanceof Error ? err.message : String(err),
    }
  }
}

function pad(value: string | number, width: number, alignRight = false): string {
  const text = String(value)
  return alignRight ? text.padStart(width) : text.padEnd(width)
}

function renderTable(budgets: InstallationBudget[]): void {
  const accountWidth = Math.max(
    7,
    ...budgets.map((budget) => (budget.installation.accountLogin ?? 'unknown').length),
  )
  const columns: [string | number, number, boolean][][] = budgets.map((budget) => [
    [budget.installation.id, 12, false],
    [budget.installation.accountLogin ?? 'unknown', accountWidth, false],
    [budget.installation.accountType ?? '-', 12, false],
    [budget.installation.repositorySelection ?? '-', 10, false],
    [budget.repoCount ?? '-', 7, true],
    [budget.graphql?.limit ?? '-', 10, true],
    [budget.graphql?.remaining ?? '-', 10, true],
    [budget.core?.limit ?? '-', 11, true],
    [budget.error ?? (budget.installation.suspendedAt ? 'suspended' : ''), 0, false],
  ])

  const header = [
    pad('installation', 12),
    pad('account', accountWidth),
    pad('type', 12),
    pad('selection', 10),
    pad('repos', 7, true),
    pad('gql limit', 10, true),
    pad('remaining', 10, true),
    pad('core limit', 11, true),
    'note',
  ].join('  ')
  process.stdout.write(`${header}\n${'-'.repeat(header.length)}\n`)
  for (const row of columns) {
    process.stdout.write(`${row.map(([v, w, r]) => pad(v, w, r)).join('  ')}\n`)
  }
}

function renderTotals(budgets: InstallationBudget[]): void {
  const usable = budgets.filter((budget) => budget.graphql && !budget.installation.suspendedAt)
  const sum = (pick: (budget: InstallationBudget) => number | null | undefined): number =>
    usable.reduce((total, budget) => total + (pick(budget) ?? 0), 0)

  const graphqlCeiling = sum((budget) => budget.graphql?.limit)
  const repos = sum((budget) => budget.repoCount)
  const failed = budgets.filter((budget) => budget.error).length

  process.stdout.write(
    `${[
      '',
      `installations discovered   ${budgets.length}`,
      `installations usable       ${usable.length}`,
      `installations failed       ${failed}`,
      `repositories reachable     ${repos}`,
      '',
      `GraphQL points / hour      ${graphqlCeiling.toLocaleString()}`,
      `GraphQL points available   ${sum((budget) => budget.graphql?.remaining).toLocaleString()}`,
      `REST core points / hour    ${sum((budget) => budget.core?.limit).toLocaleString()}`,
      `GraphQL points / day       ${(graphqlCeiling * 24).toLocaleString()}`,
      '',
    ].join('\n')}\n`,
  )
}

setImmediate(async () => {
  try {
    const { integrationId, concurrency, asJson, includeSuspended } = parseArgs(
      process.argv.slice(2),
    )

    const db = await getDbConnection(WRITE_DB_CONFIG())
    const credential = await getCredential(pgpQx(db), integrationId)

    const discovered = await listInstallations(credential)
    const installations = includeSuspended
      ? discovered
      : discovered.filter((installation) => !installation.suspendedAt)
    log.info(
      { discovered: discovered.length, auditing: installations.length, concurrency },
      'auditing github app installations',
    )

    const budgets = await mapWithConcurrency(installations, concurrency, (installation) =>
      auditInstallation(credential, installation),
    )
    budgets.sort((a, b) => (b.graphql?.limit ?? -1) - (a.graphql?.limit ?? -1))

    if (asJson) {
      process.stdout.write(`${JSON.stringify(budgets, null, 2)}\n`)
    } else {
      renderTable(budgets)
    }
    renderTotals(budgets)

    process.exit(0)
  } catch (err) {
    log.error(err, 'github app budget audit failed')
    process.exit(1)
  }
})
