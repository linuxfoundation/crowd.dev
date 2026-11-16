import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('delete-github-nango-connections')

const USAGE = `Delete every GitHub connection in Nango Cloud (linuxfoundation/lfx-cdp-insights-ops#1123)

Lists the connections of the Nango "github" integration and deletes the repo connections first,
then the github-token-* connections their syncs use. Jira, Confluence and Gerrit connections
are never touched. Dry run unless --execute is given. Safe to run again after a failure: the
list is read from Nango on every run, and a connection that is already gone counts as done.

Usage:
  npx tsx src/bin/delete-github-nango-connections.ts [options]
  (or pnpm run --silent delete-github-nango-connections [options])

Options:
  --census           Read only: count the connections of every Nango integration and exit
  --execute          Delete. Without it the script only prints what it would delete
  --limit N          Delete at most N connections (canary run)
  --delay-ms N       Pause between deletes (default 300)

Requires NANGO_CLOUD_SECRET_KEY.`

const NANGO_API_URL = 'https://api.nango.dev'
const GITHUB_INTEGRATION = 'github'
const PAGE_SIZE = 1000
const PAGE_DELAY_MS = 1000
const MAX_RATE_LIMIT_RETRIES = 8
const DEFAULT_RETRY_AFTER_SEC = 30
const PROGRESS_EVERY = 250

interface Options {
  census: boolean
  execute: boolean
  limit: number
  delayMs: number
}

interface NangoConnection {
  connection_id: string
  provider_config_key: string
}

type DeleteOutcome = 'deleted' | 'already_gone'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const isTokenConnection = (connectionId: string) =>
  connectionId.toLowerCase().startsWith('github-token-')

function parseInteger(flag: string, value: string | undefined, min: number): number {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed < min) {
    throw new Error(`${flag} needs an integer of at least ${min}, got ${value}`)
  }
  return parsed
}

function parseOptions(argv: string[]): Options {
  const options: Options = { census: false, execute: false, limit: 0, delayMs: 300 }
  for (let i = 0; i < argv.length; i++) {
    switch (argv[i]) {
      case '--census':
        options.census = true
        break
      case '--execute':
        options.execute = true
        break
      case '--limit':
        options.limit = parseInteger('--limit', argv[++i], 1)
        break
      case '--delay-ms':
        options.delayMs = parseInteger('--delay-ms', argv[++i], 0)
        break
      default:
        console.error(USAGE)
        throw new Error(`unknown argument: ${argv[i]}`)
    }
  }
  return options
}

async function withRateLimitRetry(label: string, request: () => Promise<Response>) {
  for (let attempt = 0; ; attempt++) {
    const response = await request()
    if (response.status !== 429 || attempt >= MAX_RATE_LIMIT_RETRIES) {
      return response
    }
    const retryAfterSec = Number(response.headers.get('retry-after')) || DEFAULT_RETRY_AFTER_SEC
    log.warn({ label, retryAfterSec }, 'Rate limited by Nango, retrying')
    await sleep(retryAfterSec * 1000)
  }
}

async function listConnections(
  headers: Record<string, string>,
  integration?: string,
): Promise<NangoConnection[]> {
  const filter = integration ? `integrationId=${integration}&` : ''
  const connections: NangoConnection[] = []
  for (let page = 0; ; page++) {
    const url = `${NANGO_API_URL}/connections?${filter}limit=${PAGE_SIZE}&page=${page}`
    const response = await withRateLimitRetry(`list page ${page}`, () => fetch(url, { headers }))
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} listing page ${page}: ${await response.text()}`)
    }
    const { connections: batch } = (await response.json()) as { connections: NangoConnection[] }
    connections.push(...batch)
    if (batch.length < PAGE_SIZE) {
      return connections
    }
    await sleep(PAGE_DELAY_MS)
  }
}

function censusKey(connection: NangoConnection): string {
  return connection.provider_config_key === GITHUB_INTEGRATION &&
    isTokenConnection(connection.connection_id)
    ? `${GITHUB_INTEGRATION} (token)`
    : connection.provider_config_key
}

async function logCensus(headers: Record<string, string>): Promise<void> {
  const connections = await listConnections(headers)
  const counts: Record<string, number> = {}
  for (const connection of connections) {
    const key = censusKey(connection)
    counts[key] = (counts[key] ?? 0) + 1
  }
  log.info({ total: connections.length, counts }, 'Nango connection census')
}

function orderTargets(connections: NangoConnection[]) {
  const ids = connections
    .filter((c) => c.provider_config_key === GITHUB_INTEGRATION)
    .map((c) => c.connection_id)
  const unique = [...new Set(ids)]
  if (unique.length !== ids.length) {
    throw new Error(`${ids.length - unique.length} duplicate connection ids in the listing`)
  }
  const repos = unique.filter((id) => !isTokenConnection(id))
  const tokens = unique.filter(isTokenConnection)
  return { repos, tokens, ordered: [...repos, ...tokens] }
}

async function isAlreadyGone(response: Response): Promise<boolean> {
  if (response.status === 404) {
    return true
  }
  if (response.status !== 400) {
    return false
  }
  const body = (await response
    .clone()
    .json()
    .catch(() => null)) as { error?: { code?: string } } | null
  return body?.error?.code === 'unknown_connection'
}

async function deleteConnection(
  headers: Record<string, string>,
  connectionId: string,
): Promise<DeleteOutcome> {
  const url = `${NANGO_API_URL}/connections/${encodeURIComponent(connectionId)}?provider_config_key=${GITHUB_INTEGRATION}`
  const response = await withRateLimitRetry(connectionId, () =>
    fetch(url, { method: 'DELETE', headers }),
  )
  if (response.ok) {
    return 'deleted'
  }
  if (await isAlreadyGone(response)) {
    return 'already_gone'
  }
  throw new Error(`HTTP ${response.status} deleting ${connectionId}: ${await response.text()}`)
}

async function deleteGithubConnections(
  headers: Record<string, string>,
  options: Options,
): Promise<void> {
  const { repos, tokens, ordered } = orderTargets(
    await listConnections(headers, GITHUB_INTEGRATION),
  )
  const targets = options.limit > 0 ? ordered.slice(0, options.limit) : ordered
  log.info(
    {
      repoConnections: repos.length,
      tokenConnections: tokens.length,
      targeted: targets.length,
      mode: options.execute ? 'execute' : 'dry-run',
      first: targets.slice(0, 5),
      last: targets.slice(-5),
    },
    'GitHub Nango connections to delete',
  )
  if (!options.execute) {
    return
  }

  const counts: Record<DeleteOutcome, number> = { deleted: 0, already_gone: 0 }
  for (const [index, connectionId] of targets.entries()) {
    const outcome = await deleteConnection(headers, connectionId)
    counts[outcome]++
    log.info({ connectionId, outcome }, 'Connection processed')
    if ((index + 1) % PROGRESS_EVERY === 0) {
      log.info({ processed: index + 1, total: targets.length, counts }, 'Progress')
    }
    await sleep(options.delayMs)
  }
  log.info({ processed: targets.length, counts }, 'Done')
}

async function main(): Promise<void> {
  const options = parseOptions(process.argv.slice(2))
  const secretKey = process.env.NANGO_CLOUD_SECRET_KEY
  if (!secretKey) {
    throw new Error('NANGO_CLOUD_SECRET_KEY is not set')
  }
  const headers = { Authorization: `Bearer ${secretKey}` }

  if (options.census) {
    await logCensus(headers)
  } else {
    await deleteGithubConnections(headers, options)
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    log.error(err, 'GitHub Nango connection deletion failed')
    process.exit(1)
  })
