import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { timeout } from '@crowd/common'
import type { Logger } from '@crowd/logging'

import type { ConnectorHttp } from '../../http/client'
import { ConnectorCodeError, ProviderContractError } from '../../http/errors'
import {
  CREDLY_API_BASE_URL,
  CREDLY_MIN_CALL_INTERVAL_MS,
  fetchBadgeTemplatesPage,
  fetchBadgesPage,
  resetCredlyPacing,
} from './apiClient'

vi.mock('@crowd/common', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@crowd/common')>()),
  timeout: vi.fn().mockResolvedValue(undefined),
}))

const mockedTimeout = vi.mocked(timeout)
const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() } as unknown as Logger

const WINDOW = { min: '2026-10-20T00:00:00.000Z', max: '2026-10-21T00:00:00.000Z' }

function badge(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: 'badge-1',
    state: 'accepted',
    issued_at: '2026-10-20T08:00:00.000Z',
    expires_at: null,
    state_updated_at: '2026-10-20T09:00:00.000Z',
    recipient_email: 'someone@example.org',
    issued_to: 'Someone',
    badge_url: 'https://www.credly.com/badges/badge-1',
    user: { id: 'user-1', url: 'https://www.credly.com/users/someone' },
    badge_template: { id: 'template-1', name: 'LFS158: Introduction to Kubernetes' },
    ...overrides,
  }
}

function pageOf(data: unknown[]): Record<string, unknown> {
  return {
    data,
    metadata: {
      count: data.length,
      current_page: 1,
      total_count: data.length,
      total_pages: 1,
      next_page_url: null,
    },
  }
}

function httpReturning(body: unknown): ConnectorHttp & { request: ReturnType<typeof vi.fn> } {
  return { request: vi.fn().mockResolvedValue(body), requestCount: () => 1 }
}

beforeEach(() => {
  resetCredlyPacing()
  mockedTimeout.mockClear()
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-10-22T10:00:00.000Z'))
})

afterEach(() => {
  vi.useRealTimers()
})

describe('fetchBadgesPage', () => {
  it('requests a closed state_updated_at window sorted ascending and parses the page', async () => {
    const http = httpReturning(pageOf([badge()]))

    const page = await fetchBadgesPage(
      http,
      { organizationId: 'org-1', window: WINDOW, page: 2 },
      log,
    )

    expect(page.data).toHaveLength(1)
    expect(page.data[0].id).toBe('badge-1')
    expect(page.metadata.total_count).toBe(1)
    expect(http.request).toHaveBeenCalledWith(
      expect.objectContaining({
        method: 'get',
        url: `${CREDLY_API_BASE_URL}/organizations/org-1/badges`,
        params: {
          'filter[state_updated_at_min]': '2026-10-20T00:00:00Z',
          'filter[state_updated_at_max]': '2026-10-21T00:00:00Z',
          sort: 'state_updated_at',
          per: 50,
          page: 2,
        },
      }),
      log,
      3,
    )
  })

  it('adds state and expires_at_max only as extra filters inside the window', async () => {
    const http = httpReturning(pageOf([]))

    await fetchBadgesPage(
      http,
      {
        organizationId: 'org-1',
        window: WINDOW,
        page: 1,
        per: 1,
        states: ['accepted', 'replaced'],
        expiresAtMax: '2026-10-22T00:00:00.000Z',
      },
      log,
    )

    expect(http.request.mock.calls[0][0].params).toEqual({
      'filter[state_updated_at_min]': '2026-10-20T00:00:00Z',
      'filter[state_updated_at_max]': '2026-10-21T00:00:00Z',
      'filter[state]': 'accepted,replaced',
      'filter[expires_at_max]': '2026-10-22T00:00:00.000Z',
      sort: 'state_updated_at',
      per: 1,
      page: 1,
    })
  })

  it.each([
    ['inverted', { min: WINDOW.max, max: WINDOW.min }],
    ['empty', { min: WINDOW.min, max: WINDOW.min }],
    ['unparseable', { min: 'yesterday', max: WINDOW.max }],
    ['sub-second', { min: '2026-10-20T00:00:00.200Z', max: '2026-10-20T00:00:00.900Z' }],
  ])('refuses an %s window before calling credly', async (_label, window) => {
    const http = httpReturning(pageOf([]))

    const failure = await fetchBadgesPage(
      http,
      { organizationId: 'org-1', window, page: 1 },
      log,
    ).catch((err) => err)

    expect(failure).toBeInstanceOf(ConnectorCodeError)
    expect(failure.message).toContain('state_updated_at window')
    expect(http.request).not.toHaveBeenCalled()
  })

  it('truncates the bounds to whole seconds and judges badges against those', async () => {
    const http = httpReturning(pageOf([badge({ state_updated_at: '2026-10-20T00:00:00.000Z' })]))

    await expect(
      fetchBadgesPage(
        http,
        {
          organizationId: 'org-1',
          window: { min: '2026-10-20T00:00:00.500Z', max: '2026-10-21T00:00:00.500Z' },
          page: 1,
        },
        log,
      ),
    ).resolves.toBeDefined()
    expect(http.request.mock.calls[0][0].params['filter[state_updated_at_min]']).toBe(
      '2026-10-20T00:00:00Z',
    )
  })

  it('tolerates a badge without state_updated_at instead of failing the page', async () => {
    const http = httpReturning(pageOf([badge({ state_updated_at: null })]))

    const page = await fetchBadgesPage(
      http,
      { organizationId: 'org-1', window: WINDOW, page: 1 },
      log,
    )

    expect(page.data[0].state_updated_at).toBeNull()
  })

  it('accepts a badge stamped inside the last second of the window', async () => {
    const http = httpReturning(
      pageOf([badge({ id: 'badge-2', state_updated_at: '2026-10-21T00:00:00.923Z' })]),
    )

    const page = await fetchBadgesPage(
      http,
      { organizationId: 'org-1', window: WINDOW, page: 1 },
      log,
    )

    expect(page.data[0].id).toBe('badge-2')
  })

  it('fails when a returned badge sits outside the window, since credly ignores unknown filters', async () => {
    const http = httpReturning(
      pageOf([badge(), badge({ id: 'badge-2', state_updated_at: '2026-10-21T00:00:01.000Z' })]),
    )

    const failure = await fetchBadgesPage(
      http,
      { organizationId: 'org-1', window: WINDOW, page: 1 },
      log,
    ).catch((err) => err)

    expect(failure).toBeInstanceOf(ProviderContractError)
    expect(failure.message).toContain('badge-2')
  })

  it('fails on a page that does not match the badge shape, naming the field but not the body', async () => {
    const http = httpReturning(pageOf([badge({ recipient_email: undefined })]))

    const failure = await fetchBadgesPage(
      http,
      { organizationId: 'org-1', window: WINDOW, page: 1 },
      log,
    ).catch((err) => err)

    expect(failure).toBeInstanceOf(ProviderContractError)
    expect(failure.message).toContain('data.0.recipient_email')
    expect(failure.message).not.toContain('someone@example.org')
  })

  it('spaces consecutive calls by the minimum interval', async () => {
    const http = httpReturning(pageOf([]))

    await fetchBadgesPage(http, { organizationId: 'org-1', window: WINDOW, page: 1 }, log)
    await fetchBadgesPage(http, { organizationId: 'org-1', window: WINDOW, page: 2 }, log)

    expect(mockedTimeout).toHaveBeenCalledTimes(1)
    expect(mockedTimeout).toHaveBeenCalledWith(CREDLY_MIN_CALL_INTERVAL_MS)
  })
})

describe('fetchBadgeTemplatesPage', () => {
  it('requests the templates with the state filter and parses reporting tags', async () => {
    const http = httpReturning(
      pageOf([
        {
          id: 'template-1',
          name: 'LFS158: Introduction to Kubernetes',
          state: 'active',
          type_category: 'Learning',
          reporting_tags: ['eLearning'],
        },
      ]),
    )

    const page = await fetchBadgeTemplatesPage(
      http,
      { organizationId: 'org-1', page: 1, state: 'active' },
      log,
    )

    expect(page.data[0].reporting_tags).toEqual(['eLearning'])
    expect(http.request.mock.calls[0][0]).toMatchObject({
      url: `${CREDLY_API_BASE_URL}/organizations/org-1/badge_templates`,
      params: { 'filter[state]': 'active', per: 50, page: 1 },
    })
  })
})
