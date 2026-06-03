import { FetchError } from './types'

const GRAPHQL_URL = 'https://api.github.com/graphql'

// One GraphQL call probes three common Dockerfile locations. object(expression:)
// returns null when the path doesn't exist, so a single request answers "does
// this repo ship a Dockerfile anywhere we care about?" without listing the tree.
const DOCKERFILE_QUERY = `
  query($owner: String!, $name: String!) {
    repository(owner: $owner, name: $name) {
      d0: object(expression: "HEAD:Dockerfile")        { oid }
      d1: object(expression: "HEAD:docker/Dockerfile") { oid }
      d2: object(expression: "HEAD:build/Dockerfile")  { oid }
    }
  }
`

export async function detectDockerfile(
  owner: string,
  name: string,
  token: string,
): Promise<boolean> {
  let response: Response
  try {
    response = await fetch(GRAPHQL_URL, {
      method: 'POST',
      headers: {
        Authorization: `bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: DOCKERFILE_QUERY, variables: { owner, name } }),
    })
  } catch (err) {
    throw new FetchError(
      'TRANSIENT',
      `Network error for ${owner}/${name}: ${(err as Error).message}`,
    )
  }

  const resetSec = parseInt(response.headers.get('x-ratelimit-reset') ?? '0', 10)
  const resetMs = resetSec ? resetSec * 1000 + 5_000 : Date.now() + 65_000

  if (response.status === 401) {
    throw new FetchError('AUTH', `401 Unauthorized for ${owner}/${name}`)
  }

  if (response.status === 403) {
    const body = await response.text()
    if (body.toLowerCase().includes('rate limit')) {
      throw new FetchError('RATE_LIMIT', `Rate limited on ${owner}/${name}`, resetMs)
    }
    throw new FetchError('AUTH', `403 Forbidden for ${owner}/${name}`)
  }

  if (response.status === 404) throw new FetchError('NOT_FOUND', `404 for ${owner}/${name}`)
  if (response.status >= 500) {
    throw new FetchError('TRANSIENT', `${response.status} for ${owner}/${name}`)
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let json: any
  try {
    json = await response.json()
  } catch (err) {
    throw new FetchError(
      'MALFORMED',
      `Non-JSON body for ${owner}/${name}: ${(err as Error).message}`,
    )
  }

  if (json.errors?.length) {
    const err = json.errors[0]
    if (err.type === 'RATE_LIMITED') {
      throw new FetchError('RATE_LIMIT', `RATE_LIMITED for ${owner}/${name}`, resetMs)
    }
    if (err.type === 'NOT_FOUND') {
      throw new FetchError('NOT_FOUND', `NOT_FOUND for ${owner}/${name}`)
    }
    throw new FetchError(
      'TRANSIENT',
      `GraphQL error for ${owner}/${name}: ${err.message ?? err.type}`,
    )
  }

  const repo = json.data?.repository
  if (!repo) throw new FetchError('NOT_FOUND', `No repository data for ${owner}/${name}`)

  return Boolean(repo.d0 ?? repo.d1 ?? repo.d2)
}
