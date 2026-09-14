import https from 'https'
import { Readable } from 'stream'

import { getServiceLogger } from '@crowd/logging'

import { IDatasetDescriptor, IDiscoverySource, IDiscoverySourceRow } from '../types'

const log = getServiceLogger()

const CATEGORY_SLUG = 'project-onboardings'
const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql'
const GITHUB_NON_REPO_OWNERS = new Set(['user-attachments', 'orgs', 'apps', 'marketplace'])
const OWNER = 'linuxfoundation'
const REPO = 'insights'

interface GraphQLResponse<T> {
  data?: T
  errors?: Array<{ message: string }>
}

interface DiscussionNode {
  number: number
  body: string
  closed: boolean
}

interface DiscussionsPage {
  pageInfo: { hasNextPage: boolean; endCursor: string | null }
  nodes: DiscussionNode[]
}

interface DiscussionsData {
  repository: {
    discussions: DiscussionsPage
  }
}

async function graphqlRequest<T>(query: string, variables: Record<string, unknown>): Promise<T> {
  const raw = process.env.CROWD_GITHUB_PERSONAL_ACCESS_TOKENS
  if (!raw) {
    throw new Error('CROWD_GITHUB_PERSONAL_ACCESS_TOKENS environment variable is not set')
  }
  const token = raw.split(',')[0].trim()

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

// Extracts github.com/{owner}/{repo} URLs from markdown text, normalised to the repo root.
function extractRepoUrls(text: string): string[] {
  const urls = new Set<string>()
  const regex = /https?:\/\/github\.com\/([a-zA-Z0-9_.-]+)\/([a-zA-Z0-9_.-]+)/gi
  let match: RegExpExecArray | null
  while ((match = regex.exec(text)) !== null) {
    const owner = match[1].toLowerCase()
    const repo = match[2]
      .replace(/[.,;:!?]+$/, '')
      .replace(/\.git$/, '')
      .toLowerCase()
    if (owner && repo && !GITHUB_NON_REPO_OWNERS.has(owner)) {
      urls.add(`https://github.com/${owner}/${repo}`)
    }
  }
  return Array.from(urls)
}

async function getDiscussionCategoryId(): Promise<string> {
  const query = `
    query {
      repository(owner: "${OWNER}", name: "${REPO}") {
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

  const data = await graphqlRequest<CategoriesData>(query, {})
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
    query GetDiscussions($categoryId: ID!, $cursor: String) {
      repository(owner: "${OWNER}", name: "${REPO}") {
        discussions(first: 100, categoryId: $categoryId, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            number
            body
            closed
          }
        }
      }
    }
  `

  const data = await graphqlRequest<DiscussionsData>(query, { categoryId, cursor })
  return data.repository.discussions
}

async function fetchAllDiscussionRepoUrls(): Promise<string[]> {
  const categoryId = await getDiscussionCategoryId()
  log.info({ categoryId, owner: OWNER, repo: REPO }, 'Insights Discussions: category ID resolved.')

  const allUrls = new Set<string>()
  let cursor: string | null = null
  let hasNextPage = true
  let pageCount = 0

  while (hasNextPage) {
    pageCount++
    const page = await fetchDiscussionsPage(categoryId, cursor)

    for (const discussion of page.nodes) {
      for (const url of extractRepoUrls(discussion.body)) {
        allUrls.add(url)
      }
    }

    hasNextPage = page.pageInfo.hasNextPage
    cursor = page.pageInfo.endCursor

    log.info(
      {
        pageCount,
        discussionsInPage: page.nodes.length,
        totalUniqueUrls: allUrls.size,
        hasNextPage,
      },
      'Insights Discussions: page processed.',
    )
  }

  return Array.from(allUrls)
}

export class InsightsDiscussionsSource implements IDiscoverySource {
  public readonly name = 'insights-discussions'
  public readonly format = 'json' as const

  async listAvailableDatasets(): Promise<IDatasetDescriptor[]> {
    const today = new Date().toISOString().slice(0, 10)
    return [
      {
        id: today,
        date: today,
        url: `https://github.com/${OWNER}/${REPO}/discussions/categories/${CATEGORY_SLUG}`,
      },
    ]
  }

  async fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable> {
    log.info({ datasetId: dataset.id }, 'Insights Discussions: fetching discussion repo URLs.')

    const repoUrls = await fetchAllDiscussionRepoUrls()

    log.info(
      { datasetId: dataset.id, count: repoUrls.length },
      'Insights Discussions: unique repo URLs extracted.',
    )

    return Readable.from(
      repoUrls.map((url) => ({ repoUrl: url })),
      { objectMode: true },
    )
  }

  parseRow(rawRow: Record<string, unknown>): IDiscoverySourceRow | null {
    const repoUrl = rawRow['repoUrl'] as string | undefined
    if (!repoUrl) return null

    let projectSlug = ''
    let repoName = ''
    try {
      const urlPath = new URL(repoUrl).pathname.replace(/^\//, '').replace(/\/$/, '')
      projectSlug = urlPath
      repoName = urlPath.split('/').pop() || ''
    } catch {
      return null
    }

    if (!projectSlug || !repoName) return null

    return {
      projectSlug,
      repoName,
      repoUrl,
    }
  }
}
