import https from 'https'
import { Readable } from 'stream'

import { deriveProjectIdentityFromRepoUrl } from '@crowd/data-access-layer'
import { IRequestHints } from '@crowd/data-access-layer/src/project-catalog/types'
import { getServiceLogger } from '@crowd/logging'
import {
  IExpandedOrganization,
  OrganizationRepoLookup,
  ParseOnboardingRequestResult,
  RepositoryLookup,
  checkRepositoriesOnGithub,
  expandOrganizations,
  organizationFailureNote,
} from '@crowd/project-onboarding'

import { IDatasetDescriptor, IDiscoverySource, IDiscoverySourceRow } from '../types'
import { buildDiscussionRequestText } from './parse'
import { IDiscussionText, discussionHints, toDiscussionRequest } from './request'

const log = getServiceLogger()

export type DiscussionRequestParser = (text: string) => Promise<ParseOnboardingRequestResult>

export interface IDiscussionSourceDeps {
  parseRequest: DiscussionRequestParser
  lookupRepository: RepositoryLookup
  lookupOrganization: OrganizationRepoLookup
  noteOrganizationFailures: (discussionUrl: string, notes: string[]) => Promise<void>
  heartbeat: (details: Record<string, unknown>) => void
}

export function githubToken(): string {
  const raw = process.env.CROWD_GITHUB_PERSONAL_ACCESS_TOKENS
  if (!raw) {
    throw new Error('CROWD_GITHUB_PERSONAL_ACCESS_TOKENS environment variable is not set')
  }
  return raw.split(',')[0].trim()
}

const CATEGORY_SLUG = 'project-onboardings'
const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql'
const OWNER = 'linuxfoundation'
const REPO = 'insights'

interface GraphQLResponse<T> {
  data?: T
  errors?: Array<{ message: string }>
}

export interface IDiscussionNode extends IDiscussionText {
  number: number
  url: string
  closed: boolean
  updatedAt: string
}

interface DiscussionsPage {
  pageInfo: { hasNextPage: boolean; endCursor: string | null }
  nodes: IDiscussionNode[]
}

interface DiscussionsData {
  repository: {
    discussions: DiscussionsPage
  }
}

async function graphqlRequest<T>(query: string, variables: Record<string, unknown>): Promise<T> {
  const token = githubToken()
  const body = JSON.stringify({ query, variables })

  return new Promise((resolve, reject) => {
    const url = new URL(GITHUB_GRAPHQL_URL)
    const req = https.request(
      {
        hostname: url.hostname,
        path: url.pathname,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body),
          Authorization: `Bearer ${token}`,
          'User-Agent': 'crowd-dev-discovery-worker',
        },
      },
      (res) => {
        const chunks: Uint8Array[] = []
        res.on('data', (chunk: Uint8Array) => chunks.push(chunk))
        res.on('end', () => {
          try {
            const response = JSON.parse(
              Buffer.concat(chunks).toString('utf8'),
            ) as GraphQLResponse<T>
            if (response.errors?.length) {
              reject(
                new Error(
                  `GitHub GraphQL errors: ${response.errors.map((e) => e.message).join(', ')}`,
                ),
              )
              return
            }
            if (!response.data) {
              reject(new Error('GitHub GraphQL returned empty data'))
              return
            }
            resolve(response.data)
          } catch (err) {
            reject(new Error(`Failed to parse GitHub GraphQL response: ${err}`))
          }
        })
        res.on('error', reject)
      },
    )

    req.on('error', reject)
    req.write(body)
    req.end()
  })
}

async function getDiscussionCategoryId(): Promise<string> {
  const query = `
    query GetDiscussionCategoryId($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        discussionCategories(first: 25) {
          nodes {
            id
            name
            slug
          }
        }
      }
    }
  `

  interface CategoriesData {
    repository: {
      discussionCategories: {
        nodes: Array<{ id: string; name: string; slug: string }>
      }
    }
  }

  const data = await graphqlRequest<CategoriesData>(query, { owner: OWNER, name: REPO })
  const categories = data.repository.discussionCategories.nodes
  const category = categories.find((c) => c.slug === CATEGORY_SLUG)

  if (!category) {
    throw new Error(
      `Discussion category "${CATEGORY_SLUG}" not found in ${OWNER}/${REPO}. ` +
        `Available: ${categories.map((c) => `${c.name} (${c.slug})`).join(', ')}`,
    )
  }

  return category.id
}

async function fetchDiscussionsPage(
  categoryId: string,
  cursor: string | null,
): Promise<DiscussionsPage> {
  const query = `
    query GetDiscussions($owner: String!, $name: String!, $categoryId: ID!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        discussions(first: 100, categoryId: $categoryId, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            number
            title
            url
            body
            closed
            updatedAt
          }
        }
      }
    }
  `

  const data = await graphqlRequest<DiscussionsData>(query, {
    owner: OWNER,
    name: REPO,
    categoryId,
    cursor,
  })
  return data.repository.discussions
}

export interface IDiscussionRepoUrlRef {
  repoUrl: string
  discussionNumber: number
  discussionUrl: string
  requestText: string | null
  projectName: string | null
  requestHints: IRequestHints | null
}

// A parse the model may answer later fails the activity, so Temporal retries the whole walk
// with backoff: falling back here would catalogue the discussion ungrouped and never revisit it.
async function parseDiscussion(
  discussion: IDiscussionNode,
  parseRequest: DiscussionRequestParser,
): Promise<ParseOnboardingRequestResult | null> {
  const parsed = await parseRequest(buildDiscussionRequestText(discussion))
  if (parsed.ok === true) {
    return parsed
  }
  if (parsed.retryable) {
    throw new Error(
      `Insights Discussions: parsing discussion #${discussion.number} failed: ${parsed.reason}`,
    )
  }
  log.warn(
    { number: discussion.number, reason: parsed.reason },
    'Insights Discussions: request could not be parsed, falling back to the linked repositories.',
  )
  return null
}

async function withoutMissingRepositories(
  discussion: { number: number },
  repoUrls: string[],
  bareRepoUrls: string[],
  lookupRepository: RepositoryLookup,
): Promise<string[]> {
  if (bareRepoUrls.length === 0) {
    return repoUrls
  }
  const check = await checkRepositoriesOnGithub(bareRepoUrls, lookupRepository)
  for (const { repoUrl, reason } of check.unchecked) {
    log.warn(
      { number: discussion.number, repoUrl, reason },
      'Insights Discussions: repository could not be checked on GitHub, keeping it.',
    )
  }
  if (check.missing.length > 0) {
    log.warn(
      { number: discussion.number, missing: check.missing },
      'Insights Discussions: repositories named without a link were not found on GitHub.',
    )
  }
  const missing = new Set(check.missing)
  return repoUrls.filter((repoUrl) => !missing.has(repoUrl))
}

// A failed expansion only leaves a note, and the edited discussion is read again at the next run;
// GitHub being down fails the walk instead, or an incremental run would never clear that note.
async function expandDiscussionOrganizations(
  discussion: IDiscussionNode,
  owners: string[],
  deps: IDiscussionSourceDeps,
): Promise<IExpandedOrganization[]> {
  const expansion = await expandOrganizations(owners, deps.lookupOrganization)
  if (expansion.ok === true) {
    await deps.noteOrganizationFailures(discussion.url, [])
    return expansion.organizations
  }
  log.info(
    { number: discussion.number, owners, reason: expansion.reason },
    'Insights Discussions: organization request was not expanded.',
  )
  await deps.noteOrganizationFailures(discussion.url, [organizationFailureNote(expansion)])
  if (expansion.kind === 'unavailable') {
    throw new Error(
      `Insights Discussions: expanding organizations of discussion #${discussion.number} failed: ${expansion.reason}`,
    )
  }
  return []
}

export async function collectDiscussionRefs(
  discussion: IDiscussionNode,
  deps: IDiscussionSourceDeps,
  known: { has(repoUrl: string): boolean },
): Promise<IDiscussionRepoUrlRef[]> {
  const request = toDiscussionRequest(
    discussion,
    await parseDiscussion(discussion, deps.parseRequest),
  )
  if (request.unlistedRepoUrls.length > 0) {
    log.warn(
      { number: discussion.number, unlisted: request.unlistedRepoUrls },
      'Insights Discussions: linked repositories were left out by the request parser.',
    )
  }
  const organizations = await expandDiscussionOrganizations(discussion, request.organizations, deps)
  const listed = await withoutMissingRepositories(
    discussion,
    request.repoUrls,
    request.bareRepoUrls,
    deps.lookupRepository,
  )
  const repoUrls = [...new Set([...listed, ...organizations.flatMap((org) => org.repoUrls)])]
  const fresh = repoUrls.filter((repoUrl) => !known.has(repoUrl))
  const hintsByRepoUrl = discussionHints(fresh, request.parsed, organizations)

  if (repoUrls.length === 0) {
    log.warn(
      { number: discussion.number, url: discussion.url },
      'Insights Discussions: open discussion produced no repo references.',
    )
  }

  log.info(
    {
      number: discussion.number,
      closed: discussion.closed,
      candidates: repoUrls.length,
      linked: request.linkedRepoCount,
      organizations: organizations.map((org) => org.owner),
      projectName: request.projectName,
      grouped: hintsByRepoUrl.size,
    },
    'Insights Discussions: discussion processed.',
  )

  return fresh.map((repoUrl) => ({
    repoUrl,
    discussionNumber: discussion.number,
    discussionUrl: discussion.url,
    requestText: request.requestText,
    projectName: request.projectName,
    requestHints: hintsByRepoUrl.get(repoUrl) ?? null,
  }))
}

async function fetchAllDiscussionRepoUrls(
  deps: IDiscussionSourceDeps,
  since?: string,
): Promise<IDiscussionRepoUrlRef[]> {
  const categoryId = await getDiscussionCategoryId()
  log.info(
    { categoryId, owner: OWNER, repo: REPO, since: since ?? 'none (full walk)' },
    'Insights Discussions: category ID resolved.',
  )

  // Keyed by repoUrl: a repo cited across multiple discussions keeps the first as provenance.
  const refsByRepoUrl = new Map<string, IDiscussionRepoUrlRef>()
  const sinceMs = since ? new Date(since).getTime() : null
  let cursor: string | null = null
  let hasNextPage = true
  let pageCount = 0
  let discussionsSeen = 0
  let skippedClosed = 0
  let skippedUnchanged = 0
  let discussionsWithoutRefs = 0

  while (hasNextPage) {
    pageCount++
    const page = await fetchDiscussionsPage(categoryId, cursor)

    for (const discussion of page.nodes) {
      discussionsSeen++

      if (discussion.closed) {
        skippedClosed++
        continue
      }

      // Client-side, not an early-stop: pagination isn't ordered by updatedAt, so a
      // stable walk of every page is required to not silently miss edited discussions.
      if (sinceMs !== null && new Date(discussion.updatedAt).getTime() < sinceMs) {
        skippedUnchanged++
        continue
      }

      const refs = await collectDiscussionRefs(discussion, deps, refsByRepoUrl)
      if (refs.length === 0) {
        discussionsWithoutRefs++
      }
      for (const ref of refs) {
        refsByRepoUrl.set(ref.repoUrl, ref)
      }

      deps.heartbeat({ discussionsSeen, totalUniqueUrls: refsByRepoUrl.size })
    }

    hasNextPage = page.pageInfo.hasNextPage
    cursor = page.pageInfo.endCursor

    log.info(
      {
        pageCount,
        discussionsInPage: page.nodes.length,
        totalUniqueUrls: refsByRepoUrl.size,
        hasNextPage,
      },
      'Insights Discussions: page processed.',
    )
  }

  log.info(
    {
      discussionsSeen,
      skippedClosed,
      skippedUnchanged,
      discussionsWithoutRefs,
      totalUniqueUrls: refsByRepoUrl.size,
    },
    'Insights Discussions: all pages processed.',
  )

  return Array.from(refsByRepoUrl.values())
}

export class InsightsDiscussionsSource implements IDiscoverySource {
  public readonly name = 'insights-discussions'
  public readonly provenance = 'github-discussion' as const
  public readonly format = 'json' as const

  constructor(private readonly deps: IDiscussionSourceDeps) {}

  async listAvailableDatasets(options?: { since?: string }): Promise<IDatasetDescriptor[]> {
    const today = new Date().toISOString().slice(0, 10)
    return [
      {
        id: today,
        date: today,
        url: `https://github.com/${OWNER}/${REPO}/discussions/categories/${CATEGORY_SLUG}`,
        since: options?.since,
      },
    ]
  }

  async fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable> {
    log.info(
      { datasetId: dataset.id, since: dataset.since ?? 'none (full walk)' },
      'Insights Discussions: fetching discussion repo URLs.',
    )

    const refs = await fetchAllDiscussionRepoUrls(this.deps, dataset.since)

    log.info(
      { datasetId: dataset.id, count: refs.length },
      'Insights Discussions: unique repo URLs extracted.',
    )

    return Readable.from(refs, { objectMode: true })
  }

  parseRow(rawRow: Record<string, unknown>): IDiscoverySourceRow | null {
    const repoUrl = rawRow['repoUrl'] as string | undefined
    if (!repoUrl) return null

    const identity = deriveProjectIdentityFromRepoUrl(repoUrl)
    if (!identity) return null

    const discussionUrl = rawRow['discussionUrl'] as string | undefined
    const requestText = rawRow['requestText'] as string | null | undefined
    const projectName = rawRow['projectName'] as string | null | undefined
    const requestHints = rawRow['requestHints'] as IRequestHints | null | undefined

    return {
      projectSlug: identity.projectSlug,
      repoName: identity.repoName,
      repoUrl,
      sourceUrl: discussionUrl,
      requestText: requestText ?? null,
      projectName: projectName ?? null,
      requestHints: requestHints ?? null,
    }
  }
}
