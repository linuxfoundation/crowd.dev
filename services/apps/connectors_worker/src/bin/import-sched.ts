import { readFileSync } from 'fs'
import path from 'path'

import { DataSinkWorkerEmitter } from '@crowd/common_services'
import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import {
  createSegmentIntegration,
  findSegmentIntegrationId,
} from '@crowd/data-access-layer/src/integrations'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { findSubprojectsBySourceId } from '@crowd/data-access-layer/src/segments'
import { getServiceLogger } from '@crowd/logging'
import { QUEUE_CONFIG, QueueFactory } from '@crowd/queue'
import { PlatformType } from '@crowd/types'

import { readEvent, readUsers } from '../sched/api'
import { SchedCallError, SchedRateLimitError } from '../sched/client'
import { SchedEvent, buildEventActivities } from '../sched/transform'

const log = getServiceLogger()

const USAGE = `usage: pnpm run script:import-sched --keys-stdin [--events a,b] [--check-keys | --dry-run]
  keys come on stdin as "subdomain=key" lines; events default to every event in src/sched/events.json`

const EVENTS_FILE = path.join(__dirname, '../sched/events.json')

interface Args {
  events: string[] | null
  checkKeys: boolean
  dryRun: boolean
}

interface Outcome {
  subdomain: string
  status: 'done' | 'skipped' | 'failed'
  reason?: string
}

function parseArgs(argv: string[]): Args {
  const args: Args = { events: null, checkKeys: false, dryRun: false }
  let keysStdin = false
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--keys-stdin') {
      keysStdin = true
    } else if (arg === '--check-keys') {
      args.checkKeys = true
    } else if (arg === '--dry-run') {
      args.dryRun = true
    } else if (arg === '--events' && argv[i + 1]) {
      args.events = argv[++i]
        .split(',')
        .map((subdomain) => subdomain.trim())
        .filter(Boolean)
    } else {
      throw new Error(`unknown argument: ${arg}`)
    }
  }
  if (!keysStdin) {
    throw new Error('--keys-stdin is required')
  }
  if (args.checkKeys && args.dryRun) {
    throw new Error('use --check-keys or --dry-run, not both')
  }
  return args
}

function selectEvents(all: SchedEvent[], wanted: string[] | null): SchedEvent[] {
  if (!wanted) {
    return all
  }
  const bySubdomain = new Map(all.map((event) => [event.subdomain, event]))
  const unknown = wanted.filter((subdomain) => !bySubdomain.has(subdomain))
  if (unknown.length > 0) {
    throw new Error(`events not in events.json: ${unknown.join(', ')}`)
  }
  return wanted.map((subdomain) => bySubdomain.get(subdomain) as SchedEvent)
}

async function readKeys(): Promise<Map<string, string>> {
  if (process.stdin.isTTY) {
    throw new Error('pipe the keys on stdin, one "subdomain=key" line each')
  }
  let input = ''
  for await (const chunk of process.stdin) {
    input += chunk
  }
  const keys = new Map<string, string>()
  for (const line of input.split('\n')) {
    const separator = line.indexOf('=')
    if (separator > 0) {
      keys.set(line.slice(0, separator).trim(), line.slice(separator + 1).trim())
    }
  }
  return keys
}

async function checkKey(event: SchedEvent, key: string): Promise<Outcome> {
  const users = await readUsers(event.subdomain, key)
  const emails = users.filter((user) => user.email).length
  log.info({ subdomain: event.subdomain, users: users.length, emails }, 'key works')
  return { subdomain: event.subdomain, status: 'done', reason: emails > 0 ? 'ok-rw' : 'ok-ro' }
}

async function findSegmentId(qx: QueryExecutor, event: SchedEvent): Promise<string | null> {
  const segments = await findSubprojectsBySourceId(qx, event.segment.sourceId)
  return segments.find((segment) => segment.slug === event.segment.slug)?.id ?? null
}

async function importEvent(
  qx: QueryExecutor,
  emitter: DataSinkWorkerEmitter | null,
  event: SchedEvent,
  key: string,
): Promise<Outcome> {
  const segmentId = await findSegmentId(qx, event)
  if (!segmentId) {
    return { subdomain: event.subdomain, status: 'failed', reason: 'project not found' }
  }

  const data = await readEvent(event.subdomain, key)
  const { activities, skipped } = buildEventActivities(event, data)
  const counts = {
    subdomain: event.subdomain,
    sessions: data.sessions.length,
    people: data.users.length,
    emails: data.users.filter((user) => user.email).length,
    registrations: data.registrations.length,
    speakers: data.speakers.length,
    activities: activities.length,
    skipped,
  }

  if (!emitter) {
    const integrationId = await findSegmentIntegrationId(qx, segmentId, PlatformType.SCHED)
    log.info({ ...counts, integrationExists: !!integrationId }, 'dry run: nothing sent')
    return { subdomain: event.subdomain, status: 'done' }
  }

  const integrationId =
    (await findSegmentIntegrationId(qx, segmentId, PlatformType.SCHED)) ??
    (await createSegmentIntegration(qx, segmentId, PlatformType.SCHED))
  for (const activity of activities) {
    await emitter.createAndProcessActivityResult(segmentId, integrationId, activity)
  }
  log.info(counts, 'event sent')
  return { subdomain: event.subdomain, status: 'done' }
}

async function createEmitter(): Promise<DataSinkWorkerEmitter> {
  const emitter = new DataSinkWorkerEmitter(QueueFactory.createQueueService(QUEUE_CONFIG()), log)
  await emitter.init()
  return emitter
}

function summarize(outcomes: Outcome[]): Record<string, string[]> {
  const summary: Record<string, string[]> = { done: [], skipped: [], failed: [] }
  for (const outcome of outcomes) {
    summary[outcome.status].push(
      outcome.reason ? `${outcome.subdomain} (${outcome.reason})` : outcome.subdomain,
    )
  }
  return summary
}

setImmediate(async () => {
  let args: Args
  let events: SchedEvent[]
  try {
    args = parseArgs(process.argv.slice(2))
    events = selectEvents(JSON.parse(readFileSync(EVENTS_FILE, 'utf8')), args.events)
  } catch (err) {
    log.error((err as Error).message)
    log.error(USAGE)
    process.exit(1)
  }

  const outcomes: Outcome[] = []
  let rateLimited = false
  try {
    const keys = await readKeys()
    const qx = args.checkKeys ? null : pgpQx(await getDbConnection(WRITE_DB_CONFIG()))
    const emitter = args.checkKeys || args.dryRun ? null : await createEmitter()

    for (const event of events) {
      const key = keys.get(event.subdomain)
      if (!key) {
        outcomes.push({ subdomain: event.subdomain, status: 'skipped', reason: 'no key' })
        continue
      }
      try {
        outcomes.push(
          qx === null ? await checkKey(event, key) : await importEvent(qx, emitter, event, key),
        )
      } catch (err) {
        if (err instanceof SchedRateLimitError) {
          rateLimited = true
          log.error(err.message)
          break
        }
        const authFailed = err instanceof SchedCallError && err.authFailed
        outcomes.push({
          subdomain: event.subdomain,
          status: authFailed ? 'skipped' : 'failed',
          reason: authFailed ? 'bad key' : (err as Error).message,
        })
      }
    }
  } catch (err) {
    log.error({ message: (err as Error).message }, 'sched import stopped')
    process.exit(1)
  }

  const summary = summarize(outcomes)
  log.info(
    {
      done: summary.done.length,
      skipped: summary.skipped.length,
      failed: summary.failed.length,
      notReached: events.length - outcomes.length,
    },
    'sched import finished',
  )
  for (const [status, subdomains] of Object.entries(summary)) {
    if (subdomains.length > 0) {
      log.info({ subdomains }, status)
    }
  }
  process.exit(rateLimited ? 2 : summary.failed.length > 0 ? 1 : 0)
})
