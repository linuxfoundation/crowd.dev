import type { Credential } from '@crowd/connectors'
import { createHttpClient } from '@crowd/connectors'
import { fetchBadgeTemplatesPage } from '@crowd/connectors/src/connectors/credly/apiClient'
import {
  CREDLY_POOL_ENTRY_ID,
  mintCredlyAccessToken,
} from '@crowd/connectors/src/connectors/credly/oauth'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

interface Stage {
  intervalMs: number
  calls: number
}

// Starts at the pace carried over from linuxfoundation/lfx-cdp-insights-ops#864 (5s = 12/min,
// never independently verified as a ceiling) and ramps down, aborting at the first sign of trouble.
const STAGES: Stage[] = [
  { intervalMs: 5000, calls: 10 },
  { intervalMs: 3000, calls: 10 },
  { intervalMs: 2000, calls: 10 },
  { intervalMs: 1000, calls: 10 },
  { intervalMs: 500, calls: 10 },
  { intervalMs: 250, calls: 10 },
]

const MAX_TOTAL_CALLS = STAGES.reduce((sum, s) => sum + s.calls, 0)
const MAX_WALL_CLOCK_MS = 3 * 60_000

function credentialFromEnv(): Credential {
  const clientId = process.env.CROWD_CREDLY_CLIENT_ID
  const clientSecret = process.env.CROWD_CREDLY_CLIENT_SECRET
  const organizationId = process.env.CROWD_CREDLY_ORGANIZATION_ID
  if (!clientId || !clientSecret || !organizationId) {
    log.error(
      'set CROWD_CREDLY_CLIENT_ID, CROWD_CREDLY_CLIENT_SECRET and CROWD_CREDLY_ORGANIZATION_ID first',
    )
    process.exit(1)
  }
  return {
    platform: 'credly',
    kind: 'credly-oauth-client',
    data: { clientId, clientSecret, organizationId },
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function main(): Promise<void> {
  const credential = credentialFromEnv()
  const { organizationId } = credential.data as { organizationId: string }
  const { token } = await mintCredlyAccessToken(credential)

  const http = createHttpClient({
    acquireToken: async () => ({ id: CREDLY_POOL_ENTRY_ID, value: token }),
    parkToken: async () => undefined,
    invalidateToken: async () => undefined,
    log,
  })

  const startedAtMs = Date.now()
  let totalCalls = 0

  outer: for (const stage of STAGES) {
    log.info({ intervalMs: stage.intervalMs, callsPlanned: stage.calls }, 'stage start')
    for (let i = 0; i < stage.calls; i++) {
      if (Date.now() - startedAtMs > MAX_WALL_CLOCK_MS) {
        log.warn({ totalCalls }, 'wall clock cap reached, stopping')
        break outer
      }
      if (totalCalls >= MAX_TOTAL_CALLS) {
        log.warn({ totalCalls }, 'call cap reached, stopping')
        break outer
      }

      const callStartMs = Date.now()
      totalCalls += 1
      try {
        const page = await fetchBadgeTemplatesPage(
          http,
          { organizationId, page: 1, per: 1, state: 'active' },
          log,
        )
        const elapsedMs = Date.now() - callStartMs
        log.info(
          {
            event: 'probe_call_ok',
            stageIntervalMs: stage.intervalMs,
            totalCalls,
            elapsedMs,
            totalCount: page.metadata.total_count,
          },
          'call ok',
        )
      } catch (err) {
        const elapsedMs = Date.now() - callStartMs
        log.error(
          {
            event: 'probe_call_failed',
            stageIntervalMs: stage.intervalMs,
            totalCalls,
            elapsedMs,
            errMsg: err instanceof Error ? err.message : String(err),
            errorClass: (err as { errorClass?: string })?.errorClass,
            context: (err as { context?: unknown })?.context,
          },
          'call failed, stopping the ramp here',
        )
        log.info(
          { failedAtIntervalMs: stage.intervalMs, callsBeforeFailure: totalCalls - 1 },
          'RESULT: ramp stopped at this pace',
        )
        return
      }

      await sleep(stage.intervalMs)
    }
  }

  log.info({ totalCalls }, 'RESULT: completed every stage with no failure, limit not found')
}

main().catch((err) => {
  log.error({ err }, 'credly rate-limit probe crashed')
  process.exit(1)
})
