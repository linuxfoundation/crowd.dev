import { assertAuthenticated, assertNotRateLimited, doFetch, readJson } from './publicRepoClient'
import { GithubPublicClientError, GithubPublicClientErrorKind } from './types'

const GITHUB_GRAPHQL_URL = 'https://api.github.com/graphql'
const DISCUSSION_URL_PATTERN =
  /^https:\/\/github\.com\/([A-Za-z0-9_.-]+)\/([A-Za-z0-9_.-]+)\/discussions\/(\d+)\/?$/

export interface IGithubDiscussionRef {
  owner: string
  repo: string
  number: number
}

export function parseGithubDiscussionUrl(url: string): IGithubDiscussionRef | null {
  const match = DISCUSSION_URL_PATTERN.exec(url.trim())
  if (!match) {
    return null
  }
  return { owner: match[1], repo: match[2], number: Number(match[3]) }
}

export function getGithubDiscussionsToken(): string {
  const dedicated = process.env.CROWD_GITHUB_DISCUSSIONS_TOKEN?.trim()
  if (dedicated) {
    return dedicated
  }
  const shared = process.env.CROWD_GITHUB_PERSONAL_ACCESS_TOKENS?.split(',')[0]?.trim()
  if (shared) {
    return shared
  }
  throw new GithubPublicClientError(
    'CONFIG',
    'Missing CROWD_GITHUB_DISCUSSIONS_TOKEN or CROWD_GITHUB_PERSONAL_ACCESS_TOKENS configuration',
  )
}

interface IGraphqlError {
  type?: string
  message?: string
}

interface IGraphqlResponse<T> {
  data?: T
  errors?: IGraphqlError[]
}

function graphqlErrorKind(errors: IGraphqlError[]): GithubPublicClientErrorKind {
  if (errors.some((error) => error.type === 'NOT_FOUND')) {
    return 'NOT_FOUND'
  }
  if (errors.some((error) => error.type === 'RATE_LIMITED')) {
    return 'RATE_LIMIT'
  }
  return 'TRANSIENT'
}

async function graphql<T>(
  query: string,
  variables: Record<string, unknown>,
  token: string,
  what: string,
): Promise<T> {
  const response = await doFetch(GITHUB_GRAPHQL_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
      'User-Agent': 'crowd-dev-onboarding',
    },
    body: JSON.stringify({ query, variables }),
  })
  assertAuthenticated(response, what)
  await assertNotRateLimited(response, what)
  if (!response.ok) {
    throw new GithubPublicClientError('TRANSIENT', `HTTP ${response.status} ${what}`)
  }
  const body = await readJson<IGraphqlResponse<T>>(response, GITHUB_GRAPHQL_URL)
  if (body.errors?.length || !body.data) {
    const errors = body.errors ?? []
    const messages = errors.map((error) => error.message ?? error.type).join('; ')
    throw new GithubPublicClientError(
      graphqlErrorKind(errors),
      `GitHub GraphQL errors ${what}: ${messages || 'empty data'}`,
    )
  }
  return body.data
}

const DISCUSSION_ID_QUERY = `
  query($owner: String!, $repo: String!, $number: Int!) {
    repository(owner: $owner, name: $repo) {
      discussion(number: $number) { id }
    }
  }
`

export async function fetchGithubDiscussionId(
  ref: IGithubDiscussionRef,
  token: string,
): Promise<string> {
  const data = await graphql<{ repository: { discussion: { id: string } | null } | null }>(
    DISCUSSION_ID_QUERY,
    { owner: ref.owner, repo: ref.repo, number: ref.number },
    token,
    `reading discussion #${ref.number} of ${ref.owner}/${ref.repo}`,
  )
  const id = data.repository?.discussion?.id
  if (!id) {
    throw new GithubPublicClientError(
      'NOT_FOUND',
      `Discussion #${ref.number} of ${ref.owner}/${ref.repo} not found`,
    )
  }
  return id
}

const ADD_COMMENT_MUTATION = `
  mutation($discussionId: ID!, $body: String!) {
    addDiscussionComment(input: { discussionId: $discussionId, body: $body }) {
      comment { id url }
    }
  }
`

export interface IGithubDiscussionComment {
  id: string
  url: string
}

export async function addGithubDiscussionComment(
  discussionId: string,
  body: string,
  token: string,
): Promise<IGithubDiscussionComment> {
  const data = await graphql<{ addDiscussionComment: { comment: IGithubDiscussionComment } }>(
    ADD_COMMENT_MUTATION,
    { discussionId, body },
    token,
    `commenting on discussion ${discussionId}`,
  )
  return data.addDiscussionComment.comment
}

const UPDATE_COMMENT_MUTATION = `
  mutation($commentId: ID!, $body: String!) {
    updateDiscussionComment(input: { commentId: $commentId, body: $body }) {
      comment { id url }
    }
  }
`

export async function updateGithubDiscussionComment(
  commentId: string,
  body: string,
  token: string,
): Promise<IGithubDiscussionComment> {
  const data = await graphql<{ updateDiscussionComment: { comment: IGithubDiscussionComment } }>(
    UPDATE_COMMENT_MUTATION,
    { commentId, body },
    token,
    `editing discussion comment ${commentId}`,
  )
  return data.updateDiscussionComment.comment
}
