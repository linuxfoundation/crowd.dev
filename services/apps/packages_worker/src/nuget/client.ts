import axios from 'axios'

import {
  NuGetFetchError,
  NuGetRegistrationIndex,
  NuGetRegistrationPage,
  NuGetSearchItem,
} from './types'

const SERVICE_INDEX_URL = 'https://api.nuget.org/v3/index.json'

interface ServiceIndexResource {
  '@id': string
  '@type': string
}

interface ServiceIndex {
  resources: ServiceIndexResource[]
}

interface ResolvedEndpoints {
  searchBaseUrl: string
  registrationBaseUrl: string
}

let cachedEndpoints: ResolvedEndpoints | null = null

async function resolveEndpoints(): Promise<ResolvedEndpoints> {
  if (cachedEndpoints) return cachedEndpoints

  const resp = await axios.get<ServiceIndex>(SERVICE_INDEX_URL, {
    timeout: 10000,
  })

  const resources = resp.data.resources

  const findFirst = (...types: string[]): string | undefined => {
    for (const t of types) {
      const found = resources.find((r) => r['@type'] === t)
      if (found) return found['@id']
    }
    return undefined
  }

  const searchBaseUrl = findFirst('SearchQueryService/3.5.0', 'SearchQueryService')
  const registrationBaseUrl = findFirst(
    'RegistrationsBaseUrl/3.6.0',
    'RegistrationsBaseUrl/3.5.0',
    'RegistrationsBaseUrl',
  )

  if (!searchBaseUrl || !registrationBaseUrl) {
    throw new Error('NuGet service index missing required endpoints')
  }

  cachedEndpoints = { searchBaseUrl, registrationBaseUrl }
  return cachedEndpoints
}

function classifyError(err: unknown): NuGetFetchError | null {
  if (!axios.isAxiosError(err)) return null
  const status = err.response?.status
  if (status === 404) return { kind: 'NOT_FOUND', status, message: err.message }
  if (status === 429) return { kind: 'RATE_LIMIT', status, message: err.message }
  return null
}

export async function fetchSearch(packageId: string): Promise<NuGetSearchItem | NuGetFetchError> {
  const { searchBaseUrl } = await resolveEndpoints()
  const lowerPackageId = packageId.toLowerCase()

  try {
    const resp = await axios.get<{ totalHits: number; data: NuGetSearchItem[] }>(searchBaseUrl, {
      params: {
        q: `packageid:${packageId}`,
        prerelease: 'true',
        semVerLevel: '2.0.0',
        take: 20,
      },
      headers: {
        'Accept-Encoding': 'gzip',
      },
      timeout: 15000,
    })

    const match = resp.data.data.find((item) => item.id.toLowerCase() === lowerPackageId)
    if (!match) {
      return { kind: 'NOT_FOUND', message: `Package ${packageId} not found in search results` }
    }
    return match
  } catch (err) {
    const classified = classifyError(err)
    if (classified) return classified
    throw err
  }
}

async function fetchRegistrationPage(
  pageId: string,
  maxAttempts = 2,
): Promise<NuGetRegistrationPage> {
  for (let attempt = 1; ; attempt++) {
    try {
      const resp = await axios.get<NuGetRegistrationPage>(pageId, {
        headers: { 'Accept-Encoding': 'gzip' },
        timeout: 15000,
      })
      return resp.data
    } catch (err) {
      if (attempt >= maxAttempts) throw err
    }
  }
}

export async function fetchRegistration(
  packageId: string,
): Promise<NuGetRegistrationIndex | NuGetFetchError> {
  const { registrationBaseUrl } = await resolveEndpoints()
  const lowerId = packageId.toLowerCase()

  try {
    const resp = await axios.get<NuGetRegistrationIndex>(
      `${registrationBaseUrl}${lowerId}/index.json`,
      {
        headers: { 'Accept-Encoding': 'gzip' },
        timeout: 15000,
      },
    )

    const index = resp.data

    for (let i = 0; i < index.items.length; i++) {
      const page = index.items[i]
      if (!page.items) {
        const fullPage = await fetchRegistrationPage(page['@id'])
        index.items[i] = { ...page, items: fullPage.items ?? [] }
      }
    }

    return index
  } catch (err) {
    const classified = classifyError(err)
    if (classified) return classified
    throw err
  }
}
