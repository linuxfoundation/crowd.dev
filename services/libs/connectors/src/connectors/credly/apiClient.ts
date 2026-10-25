import { z } from 'zod'

import { timeout } from '@crowd/common'
import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../http/client'
import { ConnectorCodeError, ProviderContractError } from '../../http/errors'
import { credlyBadgeSchema } from './badge'
import type { CredlyBadge, CredlyBadgeState } from './badge'

export const CREDLY_API_BASE_URL = 'https://api.credly.com/v1'
export const CREDLY_PAGE_SIZE = 50
// Timings and the window rule come from the hand probes of linuxfoundation/lfx-cdp-insights-ops#864:
// a closed window answers in seconds, an unbounded scan is a 500 after ~30 s, 12 calls/min never throttled.
export const CREDLY_API_TIMEOUT_MS = 60_000
export const CREDLY_MIN_CALL_INTERVAL_MS = 5_000
const MAX_ATTEMPTS = 3

export interface StateUpdatedWindow {
  min: string
  max: string
}

export interface BadgesPageParams {
  organizationId: string
  window: StateUpdatedWindow
  page: number
  per?: number
  states?: CredlyBadgeState[]
  expiresAtMax?: string
}

export interface BadgeTemplatesPageParams {
  organizationId: string
  page: number
  per?: number
  state?: 'active' | 'archived'
}

export interface CredlyPageMetadata {
  count: number
  current_page: number
  total_count: number
  total_pages: number
  next_page_url?: string | null
}

export interface CredlyPage<T> {
  data: T[]
  metadata: CredlyPageMetadata
}

const pageMetadataSchema = z
  .object({
    count: z.number(),
    current_page: z.number(),
    total_count: z.number(),
    total_pages: z.number(),
    next_page_url: z.string().nullable().optional(),
  })
  .passthrough()

const badgesPageSchema = z.object({
  data: z.array(credlyBadgeSchema),
  metadata: pageMetadataSchema,
})

export const credlyBadgeTemplateListingSchema = z
  .object({
    id: z.string(),
    name: z.string(),
    state: z.string().nullable().optional(),
    url: z.string().nullable().optional(),
    vanity_slug: z.string().nullable().optional(),
    type_category: z.string().nullable().optional(),
    global_activity_url: z.string().nullable().optional(),
    reporting_tags: z.array(z.string()).nullable().optional(),
  })
  .passthrough()

export type CredlyBadgeTemplateListing = z.infer<typeof credlyBadgeTemplateListingSchema>

const badgeTemplatesPageSchema = z.object({
  data: z.array(credlyBadgeTemplateListingSchema),
  metadata: pageMetadataSchema,
})

export async function fetchBadgesPage(
  http: ConnectorHttp,
  params: BadgesPageParams,
  log: Logger,
): Promise<CredlyPage<CredlyBadge>> {
  const window = closedWindow(params.window)
  const body = await credlyGet(
    http,
    `/organizations/${params.organizationId}/badges`,
    {
      'filter[state_updated_at_min]': window.min,
      'filter[state_updated_at_max]': window.max,
      ...(params.states?.length && { 'filter[state]': params.states.join(',') }),
      ...(params.expiresAtMax && { 'filter[expires_at_max]': params.expiresAtMax }),
      sort: 'state_updated_at',
      per: params.per ?? CREDLY_PAGE_SIZE,
      page: params.page,
    },
    log,
  )
  const page = parsePage<CredlyBadge>(badgesPageSchema, body, 'badges')
  assertBadgesInsideWindow(page.data, window)
  return page
}

export async function fetchBadgeTemplatesPage(
  http: ConnectorHttp,
  params: BadgeTemplatesPageParams,
  log: Logger,
): Promise<CredlyPage<CredlyBadgeTemplateListing>> {
  const body = await credlyGet(
    http,
    `/organizations/${params.organizationId}/badge_templates`,
    {
      ...(params.state && { 'filter[state]': params.state }),
      per: params.per ?? CREDLY_PAGE_SIZE,
      page: params.page,
    },
    log,
  )
  return parsePage<CredlyBadgeTemplateListing>(badgeTemplatesPageSchema, body, 'badge_templates')
}

let lastCallAtMs = 0

export function resetCredlyPacing(): void {
  lastCallAtMs = 0
}

async function credlyGet(
  http: ConnectorHttp,
  path: string,
  params: Record<string, string | number>,
  log: Logger,
): Promise<unknown> {
  await pace()
  return http.request<unknown>(
    {
      method: 'get',
      url: `${CREDLY_API_BASE_URL}${path}`,
      params,
      headers: { Accept: 'application/json' },
      timeout: CREDLY_API_TIMEOUT_MS,
    },
    log,
    MAX_ATTEMPTS,
  )
}

async function pace(): Promise<void> {
  const waitMs = lastCallAtMs + CREDLY_MIN_CALL_INTERVAL_MS - Date.now()
  if (waitMs > 0) {
    await timeout(waitMs)
  }
  lastCallAtMs = Date.now()
}

// Credly ignores a filter key it does not recognise and answers with the whole organization
// (linuxfoundation/lfx-cdp-insights-ops#864), so a window is trusted only when both bounds parse
// and every returned badge sits inside it. Bounds are whole seconds: Credly filters at that grain.
function closedWindow(window: StateUpdatedWindow): StateUpdatedWindow {
  const minMs = Date.parse(window.min)
  const maxMs = Date.parse(window.max)
  if (!Number.isFinite(minMs) || !Number.isFinite(maxMs)) {
    throw new ConnectorCodeError(
      `credly badges need a parsable state_updated_at window, got ${window.min} .. ${window.max}`,
    )
  }
  const min = toWholeSecond(minMs)
  const max = toWholeSecond(maxMs)
  if (min >= max) {
    throw new ConnectorCodeError(
      `credly badges need a closed state_updated_at window, got ${window.min} .. ${window.max}`,
    )
  }
  return { min, max }
}

function toWholeSecond(ms: number): string {
  return new Date(Math.floor(ms / 1000) * 1000).toISOString().replace('.000Z', 'Z')
}

// Both bounds are whole seconds and Credly keeps the max second whole, so its last millisecond is inside.
function assertBadgesInsideWindow(badges: CredlyBadge[], window: StateUpdatedWindow): void {
  const minMs = Date.parse(window.min)
  const maxMs = Date.parse(window.max) + 999
  const outside = badges.find((badge) => {
    if (!badge.state_updated_at) return false
    const stateUpdatedAtMs = Date.parse(badge.state_updated_at)
    return (
      !Number.isFinite(stateUpdatedAtMs) || stateUpdatedAtMs < minMs || stateUpdatedAtMs > maxMs
    )
  })
  if (outside) {
    throw new ProviderContractError(
      `credly returned badge ${outside.id} with state_updated_at ${outside.state_updated_at} outside the window ${window.min} .. ${window.max}`,
    )
  }
}

function parsePage<T>(schema: z.ZodTypeAny, body: unknown, what: string): CredlyPage<T> {
  const parsed = schema.safeParse(body)
  if (!parsed.success) {
    const issues = parsed.error.issues
      .slice(0, 5)
      .map((issue) => `${issue.path.join('.')}: ${issue.message}`)
      .join('; ')
    throw new ProviderContractError(
      `credly ${what} page does not match the expected shape: ${issues}`,
    )
  }
  return parsed.data as CredlyPage<T>
}
