import type { Credential } from '@crowd/connectors'
import { createHttpClient } from '@crowd/connectors'
import {
  fetchBadgeTemplatesPage,
  fetchBadgesPage,
} from '@crowd/connectors/src/connectors/credly/apiClient'
import {
  CREDLY_POOL_ENTRY_ID,
  mintCredlyAccessToken,
} from '@crowd/connectors/src/connectors/credly/oauth'
import { getServiceLogger } from '@crowd/logging'

const log = getServiceLogger()

const WINDOW_HOURS = 24

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

async function main(): Promise<void> {
  const credential = credentialFromEnv()
  const { organizationId } = credential.data as { organizationId: string }

  const { token, expiresAt } = await mintCredlyAccessToken(credential)
  log.info({ expiresAt, tokenLength: token.length }, 'oauth token exchange ok')

  const http = createHttpClient({
    acquireToken: async () => ({ id: CREDLY_POOL_ENTRY_ID, value: token }),
    parkToken: async () => undefined,
    invalidateToken: async () => undefined,
    log,
  })

  const max = new Date()
  const min = new Date(max.getTime() - WINDOW_HOURS * 3_600_000)
  const badges = await fetchBadgesPage(
    http,
    {
      organizationId,
      window: { min: min.toISOString(), max: max.toISOString() },
      page: 1,
      per: 1,
    },
    log,
  )
  log.info(
    {
      windowHours: WINDOW_HOURS,
      totalCount: badges.metadata.total_count,
      totalPages: badges.metadata.total_pages,
      firstStateUpdatedAt: badges.data[0]?.state_updated_at ?? null,
      firstTemplate: badges.data[0]?.badge_template.name ?? null,
    },
    'badges window ok',
  )

  const templates = await fetchBadgeTemplatesPage(
    http,
    { organizationId, page: 1, per: 1, state: 'active' },
    log,
  )
  log.info(
    { activeTemplates: templates.metadata.total_count, requests: http.requestCount() },
    'badge templates ok',
  )
}

main().catch((err) => {
  log.error({ err }, 'credly probe failed')
  process.exit(1)
})
