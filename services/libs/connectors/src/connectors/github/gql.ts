import type { ConnectorHttp } from '../../http/client'
import { ProviderContractError } from '../../http/errors'

interface GraphqlError {
  type?: string
  message?: string
  path?: (string | number)[]
}

interface GraphqlEnvelope<T> {
  data?: T
  errors?: GraphqlError[]
}

// GitHub resolves what it can and reports unreachable nodes (actors in orgs with
// IP allow lists) as errors with a deep path, alongside usable data. Only errors
// at or above the root field mean the whole response is unusable.
function isFatal(error: GraphqlError): boolean {
  return (error.path?.length ?? 0) <= 1
}

export async function githubGraphql<T>(
  http: ConnectorHttp,
  query: string,
  variables: Record<string, unknown>,
): Promise<T> {
  const body = await http.request<GraphqlEnvelope<T>>({
    method: 'post',
    url: 'https://api.github.com/graphql',
    data: { query, variables },
  })
  const fatal = body.errors?.filter(isFatal) ?? []
  if (fatal.length > 0) {
    const details = fatal.map((e) => `${e.type ?? 'ERROR'}: ${e.message ?? ''}`).join('; ')
    throw new ProviderContractError(`github graphql errors: ${details}`)
  }
  if (!body.data) {
    throw new ProviderContractError('github graphql response has no data')
  }
  return body.data
}
