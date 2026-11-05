import axios from 'axios'

import { ProviderContractError } from '../../http/errors'
import {
  OCTOLENS_API_BASE_URL,
  octolensRequestConfig,
  sendOctolensRequest,
  withOctolensRetries,
} from './apiClient'

const MENTIONS_PAGE_SIZE = 100

export interface OctolensMention {
  id: number
  sourceId: string
  url: string
  title: string | null
  body: string | null
  source: string
  timestamp: string
  author: string | null
  authorName: string | null
  authorUrl: string | null
  authorAvatar: string | null
  authorFollowers: number | null
  relevance: 'relevant' | 'not_relevant'
  relevanceComment: string | null
  sentiment: 'Positive' | 'Neutral' | 'Negative' | null
  language: string | null
  tags: string[]
  keywords: OctolensMentionKeyword[]
  engaged: boolean
  isReply: boolean
  review?: OctolensMentionReview
  relevanceScore?: number | null
  feedbackRelevant?: number
  imageUrl?: string | null
  engagementMetrics?: Record<string, number>
  engagementObservedAt?: string | null
  // Optional per Octolens's spec (denormalized from the first entry of `keywords[]`, which
  // is required but could in principle be empty) — never assume it's present.
  keywordId?: number
}

export interface OctolensMentionKeyword {
  id: number
  keyword: string
  keywordTag?: string
}

export interface OctolensMentionReview {
  rating: number
  ratingMax: number
  verified: boolean | null
  response: string | null
  responseAt: string | null
  targetName: string | null
}

export interface OctolensMentionsPage {
  data: OctolensMention[]
  nextCursor: string | null
}

export async function fetchMentionsPage(
  token: string,
  params: { startDate?: string; cursor?: string; keywordIds?: number[] },
): Promise<OctolensMentionsPage> {
  return withOctolensRetries(async () =>
    parseMentionsPage(
      await sendOctolensRequest(
        () =>
          axios.post(
            `${OCTOLENS_API_BASE_URL}/mentions`,
            {
              limit: MENTIONS_PAGE_SIZE,
              cursor: params.cursor,
              filters: buildFilters(params),
              // includeAll is left at its default (false): only relevant mentions (score 0 or 1)
              // come back, so low-relevance ones never reach CDP or cost a page of the budget.
              includeEngagementMetrics: true,
            },
            octolensRequestConfig(token),
          ),
        'mentions',
      ),
    ),
  )
}

function buildFilters(params: {
  startDate?: string
  keywordIds?: number[]
}): Record<string, unknown> | undefined {
  const filters: Record<string, unknown> = {}
  if (params.startDate) filters.startDate = params.startDate
  if (params.keywordIds?.length) filters.keyword = params.keywordIds
  return Object.keys(filters).length > 0 ? filters : undefined
}

function parseMentionsPage(data: unknown): OctolensMentionsPage {
  const body = data as Partial<{
    data: OctolensMention[]
    pagination: Partial<{ nextCursor: string | null }>
  }>
  const nextCursor = body?.pagination?.nextCursor
  // An omitted field or an empty string both decode falsy, same as null — runMentionPoll's
  // `!page.nextCursor` would wrongly treat either as drained, so only null or a real cursor passes.
  if (
    !Array.isArray(body?.data) ||
    !(nextCursor === null || (typeof nextCursor === 'string' && nextCursor.length > 0))
  ) {
    throw new ProviderContractError('octolens mentions response missing data/pagination')
  }
  return { data: body.data, nextCursor }
}
