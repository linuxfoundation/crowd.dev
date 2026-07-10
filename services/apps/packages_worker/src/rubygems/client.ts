import axios from 'axios'

import {
  RubyGemsFetchError,
  RubyGemsFetchResult,
  RubyGemsGemResponse,
  RubyGemsOwner,
  RubyGemsVersionItem,
} from './types'

function classifyError(err: unknown): RubyGemsFetchError | null {
  if (!axios.isAxiosError(err)) return null
  const status = err.response?.status
  if (status === 404) return { kind: 'NOT_FOUND', status, message: err.message }
  if (status === 429) return { kind: 'RATE_LIMIT', status, message: err.message }
  return null
}

export async function fetchGem(name: string): Promise<RubyGemsFetchResult<RubyGemsGemResponse>> {
  try {
    const resp = await axios.get<RubyGemsGemResponse>(
      `https://rubygems.org/api/v1/gems/${encodeURIComponent(name)}.json`,
      { timeout: 15000 },
    )
    return resp.data
  } catch (err) {
    const classified = classifyError(err)
    if (classified) return classified
    throw err
  }
}

export async function fetchVersions(
  name: string,
): Promise<RubyGemsFetchResult<RubyGemsVersionItem[]>> {
  try {
    const resp = await axios.get<RubyGemsVersionItem[]>(
      `https://rubygems.org/api/v1/versions/${encodeURIComponent(name)}.json`,
      { timeout: 15000 },
    )
    return resp.data
  } catch (err) {
    const classified = classifyError(err)
    if (classified) return classified
    throw err
  }
}

export async function fetchOwners(name: string): Promise<RubyGemsFetchResult<RubyGemsOwner[]>> {
  try {
    const resp = await axios.get<RubyGemsOwner[]>(
      `https://rubygems.org/api/v1/gems/${encodeURIComponent(name)}/owners.json`,
      { timeout: 15000 },
    )
    return resp.data
  } catch (err) {
    const classified = classifyError(err)
    if (classified) return classified
    throw err
  }
}

export async function fetchReverseDependencies(
  name: string,
): Promise<RubyGemsFetchResult<string[]>> {
  try {
    const resp = await axios.get<string[]>(
      `https://rubygems.org/api/v1/gems/${encodeURIComponent(name)}/reverse_dependencies.json`,
      { timeout: 15000 },
    )
    return resp.data
  } catch (err) {
    const classified = classifyError(err)
    if (classified) return classified
    throw err
  }
}
