import { XMLParser } from 'fast-xml-parser'
import * as fs from 'fs'

import { fetchNuspec } from '../../nuget/client'
import { isNuGetFetchError } from '../../nuget/types'
import { canonicalizeRepoUrl } from '../../utils/canonicalizeRepoUrl'

import { downloadAndExtractTarball } from './npmTarball'

// Thrown when no GitHub source could be resolved for a dependent at all — the
// reachability stage turns this into a clean "no source" verdict rather than a retry.
export class NuGetSourceNotFoundError extends Error {
  constructor(packageId: string, version: string) {
    super(`No resolvable GitHub source for ${packageId}@${version}`)
    this.name = 'NuGetSourceNotFoundError'
  }
}

const nuspecParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' })

interface NuspecRepository {
  url: string | null
  commit: string | null
}

// Mirrors nuget/normalize.ts:parseNuspecRepositoryUrl, but also reads @_commit —
// blast-radius needs the exact commit to fetch matching C# source, not just the repo.
function parseNuspecRepository(nuspecXml: string): NuspecRepository {
  try {
    const doc = nuspecParser.parse(nuspecXml)
    const repository = doc?.package?.metadata?.repository
    const url = typeof repository?.['@_url'] === 'string' ? repository['@_url'].trim() : null
    const commit =
      typeof repository?.['@_commit'] === 'string' ? repository['@_commit'].trim() : null
    return { url: url || null, commit: commit || null }
  } catch {
    return { url: null, commit: null }
  }
}

function githubOwnerRepo(canonicalGithubUrl: string): { owner: string; repo: string } | null {
  const match = canonicalGithubUrl.match(/^https:\/\/github\.com\/([^/]+)\/([^/]+)$/)
  return match ? { owner: match[1], repo: match[2] } : null
}

function codeloadTarballUrl(owner: string, repo: string, ref: string): string {
  return `https://codeload.github.com/${owner}/${repo}/tar.gz/${ref}`
}

// .nupkg has no source (unlike Maven's -sources.jar), so fetch from GitHub repo.
// Try exact commit first, then common version-tag conventions.
async function candidateSourceTarballUrls(packageId: string, version: string): Promise<string[]> {
  const nuspec = await fetchNuspec(packageId, version)
  if (isNuGetFetchError(nuspec)) return []

  const { url, commit } = parseNuspecRepository(nuspec)
  if (!url) return []

  const canonical = canonicalizeRepoUrl(url)
  if (!canonical || canonical.host !== 'github') return []

  const ownerRepo = githubOwnerRepo(canonical.url)
  if (!ownerRepo) return []

  // An authoritative commit must never fall through to guessed tags, which could
  // resolve to a different revision and produce a verdict from mismatched source.
  if (commit) return [codeloadTarballUrl(ownerRepo.owner, ownerRepo.repo, commit)]

  return [
    codeloadTarballUrl(ownerRepo.owner, ownerRepo.repo, `v${version}`),
    codeloadTarballUrl(ownerRepo.owner, ownerRepo.repo, version),
  ]
}

export async function downloadAndExtractNuGetSource(
  packageId: string,
  version: string,
  destDir: string,
): Promise<void> {
  const candidates = await candidateSourceTarballUrls(packageId, version)
  if (candidates.length === 0) {
    throw new NuGetSourceNotFoundError(packageId, version)
  }

  let lastErr: unknown
  for (const url of candidates) {
    try {
      // Clear between attempts — a prior candidate's partial extraction (e.g. hit an
      // extraction limit mid-stream) must not leave stale files a later candidate builds on.
      fs.rmSync(destDir, { recursive: true, force: true })
      await downloadAndExtractTarball(url, destDir)
      return
    } catch (err) {
      lastErr = err
    }
  }

  throw new NuGetSourceNotFoundError(
    `${packageId} (last error: ${lastErr instanceof Error ? lastErr.message : String(lastErr)})`,
    version,
  )
}
