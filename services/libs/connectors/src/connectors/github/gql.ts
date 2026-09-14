import type { ConnectorHttp } from '../../http/client'
import { ProviderContractError } from '../../http/errors'

interface GraphqlEnvelope<T> {
  data?: T
  errors?: { type?: string; message?: string }[]
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
  if (body.errors?.length) {
    const details = body.errors.map((e) => `${e.type ?? 'ERROR'}: ${e.message ?? ''}`).join('; ')
    throw new ProviderContractError(`github graphql errors: ${details}`)
  }
  if (!body.data) {
    throw new ProviderContractError('github graphql response has no data')
  }
  return body.data
}
