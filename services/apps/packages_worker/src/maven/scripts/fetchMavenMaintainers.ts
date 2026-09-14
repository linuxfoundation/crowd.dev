/**
 * Fetches maintainer data for Maven packages from Libraries.io and GitHub.
 *
 * For each package in the input CSV it:
 *   1. Calls Libraries.io to get the linked GitHub repo URL
 *   2. Looks for CODEOWNERS or MAINTAINERS.md in the repo
 *   3. Falls back to top GitHub contributors if neither file is found
 *
 * Output: CSV with one row per (package, maintainer) ready for manual review.
 * Packages with no maintainer found still get an empty row so nothing is lost.
 *
 * Env vars:
 *   LIBRARIES_IO_API_KEY   – https://libraries.io/api_key
 *   GITHUB_TOKEN           – PAT with public_repo scope
 *
 * Usage:
 *   LIBRARIES_IO_API_KEY=xxx GITHUB_TOKEN=yyy \
 *     tsx src/maven/scripts/fetchMavenMaintainers.ts /path/to/packages_top500.csv [output.csv]
 */
import axios, { AxiosInstance } from 'axios'
import * as fs from 'fs'
import * as path from 'path'

import { extractArtifact, normalizeScmUrl } from '../extract'
import { resolveLatestVersion } from '../metadata'
import { resolveRegistryBaseUrl } from '../registry'

import { parseCsv } from './csv'

// ─── Config ───────────────────────────────────────────────────────────────────

const LIBRARIES_IO_KEY = process.env.LIBRARIES_IO_API_KEY ?? ''
const GITHUB_TOKEN = process.env.GITHUB_TOKEN ?? ''

// Libraries.io free tier: 60 req/min → 1 req/sec to stay safe
const LIBRARIES_IO_DELAY_MS = 1_100
// GitHub: 5000 req/hr with token, small courtesy delay between batches
const GITHUB_DELAY_MS = 200

// Set to 0 for unlimited (paginates all pages)
const TOP_CONTRIBUTORS_LIMIT = 0

// ─── HTTP clients ─────────────────────────────────────────────────────────────

const libIo: AxiosInstance = axios.create({
  baseURL: 'https://libraries.io/api',
  timeout: 15_000,
})

const github: AxiosInstance = axios.create({
  baseURL: 'https://api.github.com',
  timeout: 15_000,
  headers: {
    Authorization: GITHUB_TOKEN ? `Bearer ${GITHUB_TOKEN}` : undefined,
    Accept: 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
  },
})

// ─── Types ────────────────────────────────────────────────────────────────────

interface MaintainerRow {
  package_purl: string
  package_namespace: string
  package_name: string
  github_repo: string
  repo_source: string
  maintainer_github_login: string
  maintainer_display_name: string
  maintainer_email: string
  maintainer_url: string
  role: string
  source: string
  contributions: string
  notes: string
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

async function safeGet<T>(
  client: AxiosInstance,
  url: string,
  params?: Record<string, string>,
): Promise<T | null> {
  const MAX_RETRIES = 5
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const res = await client.get<T>(url, { params })
      return res.data
    } catch (err) {
      if (axios.isAxiosError(err)) {
        const status = err.response?.status
        if (status === 404 || status === 422) return null
        if (status === 403 || status === 429) {
          const reset = err.response?.headers?.['x-ratelimit-reset']
          const wait = reset ? Math.max(0, Number(reset) * 1000 - Date.now()) + 1000 : 60_000
          console.warn(
            `  Rate limited (attempt ${attempt + 1}/${MAX_RETRIES}) — waiting ${Math.round(wait / 1000)}s`,
          )
          await sleep(wait)
          continue
        }
        console.warn(`  HTTP ${status} for ${url}`)
      }
      return null
    }
  }
  console.warn(`  Giving up after ${MAX_RETRIES} rate-limit retries: ${url}`)
  return null
}

function csvEscape(value: string): string {
  const s = String(value ?? '')
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`
  }
  return s
}

function toCsvLine(row: MaintainerRow): string {
  return [
    row.package_purl,
    row.package_namespace,
    row.package_name,
    row.github_repo,
    row.repo_source,
    row.maintainer_github_login,
    row.maintainer_display_name,
    row.maintainer_email,
    row.maintainer_url,
    row.role,
    row.source,
    row.contributions,
    row.notes,
  ]
    .map(csvEscape)
    .join(',')
}

// ─── Libraries.io ─────────────────────────────────────────────────────────────

interface LibIoPackage {
  repository_url?: string
  homepage?: string
}

async function getLibIoRepoUrl(groupId: string, artifactId: string): Promise<string | null> {
  if (!LIBRARIES_IO_KEY) return null
  const data = await safeGet<LibIoPackage>(
    libIo,
    `/maven/${encodeURIComponent(`${groupId}:${artifactId}`)}`,
    { api_key: LIBRARIES_IO_KEY },
  )
  return data?.repository_url ?? null
}

// ─── POM SCM fallback ─────────────────────────────────────────────────────────

async function getPomScmUrl(groupId: string, artifactId: string): Promise<string | null> {
  const baseUrl = resolveRegistryBaseUrl(groupId)
  const version = await resolveLatestVersion(groupId, artifactId, baseUrl)
  if (!version) return null
  const extracted = await extractArtifact(groupId, artifactId, version, baseUrl)
  return normalizeScmUrl(extracted.scmUrl)
}

// ─── GitHub: parse owner/repo from URL ───────────────────────────────────────

/**
 * Converts known non-GitHub SCM URLs to their GitHub mirror equivalent.
 *
 * Apache projects publish to gitbox.apache.org (and historically git.apache.org)
 * but mirror everything to github.com/apache/. Eclipse similarly mirrors to
 * github.com/eclipse/. This lets us use the GitHub API for those repos.
 */
function toGithubUrl(url: string): string {
  // Apache Gitbox: https://gitbox.apache.org/repos/asf/ant.git → https://github.com/apache/ant
  const apacheGitbox = url.match(/gitbox\.apache\.org\/repos\/asf\/([^/.]+?)(?:\.git)?(?:\/.*)?$/)
  if (apacheGitbox) return `https://github.com/apache/${apacheGitbox[1]}`

  // Apache old-git: https://git-wip-us.apache.org/repos/asf/freemarker.git → https://github.com/apache/freemarker
  const apacheWip = url.match(/git-wip-us\.apache\.org\/repos\/asf\/([^/.]+?)(?:\.git)?(?:\/.*)?$/)
  if (apacheWip) return `https://github.com/apache/${apacheWip[1]}`

  // Apache Git (legacy): https://git.apache.org/ant.git → https://github.com/apache/ant
  const apacheGit = url.match(/(?:^|\/)git\.apache\.org\/([^/.]+?)(?:\.git)?(?:\/.*)?$/)
  if (apacheGit) return `https://github.com/apache/${apacheGit[1]}`

  // Eclipse: https://git.eclipse.org/c/platform/eclipse.platform.git → https://github.com/eclipse/eclipse.platform
  const eclipse = url.match(/git\.eclipse\.org\/c\/(?:[^/]+\/)?([^/.]+?)(?:\.git)?(?:\/.*)?$/)
  if (eclipse) return `https://github.com/eclipse/${eclipse[1]}`

  return url
}

/**
 * Hardcoded overrides for groupIds where Libraries.io consistently returns the
 * wrong repo (parent POMs, broken templates) or where the Apache heuristic
 * would produce the wrong project name.
 *
 * Key: groupId prefix (exact match or startsWith)
 * Value: GitHub owner/repo
 */
const REPO_OVERRIDES: Record<string, string> = {
  // Jersey moved from java.net to Eclipse Foundation
  'com.sun.jersey': 'eclipse-ee4j/jersey',
  'com.sun.jersey.contribs': 'eclipse-ee4j/jersey',
  'com.sun.jersey.jersey-test-framework': 'eclipse-ee4j/jersey',
  // Apache Ivy lives under ant-ivy, not ivy
  'org.apache.ivy': 'apache/ant-ivy',
  // Apache Axis 1.x
  'org.apache.axis': 'apache/axis1-java',
  // Apache Commons bare groupId — use artifactId (handled in guessApacheGithubUrl)
  // Dirigible has a broken URL template in Libraries.io
  'org.eclipse.dirigible': 'eclipse/dirigible',
  // Hudson is effectively dead/archived but let's point to the right place
  'org.eclipse.hudson': 'hudson/hudson',
  'org.jvnet.hudson.main': 'hudson/hudson',
}

/**
 * Returns a hardcoded GitHub owner/repo for known problematic groupIds,
 * or null if no override exists.
 */
function getRepoOverride(groupId: string): string | null {
  // Exact match first
  if (REPO_OVERRIDES[groupId]) return `https://github.com/${REPO_OVERRIDES[groupId]}`
  // Prefix match
  for (const prefix of Object.keys(REPO_OVERRIDES)) {
    if (groupId.startsWith(prefix + '.')) return `https://github.com/${REPO_OVERRIDES[prefix]}`
  }
  return null
}

/**
 * Returns true for repo URLs that are known to be parent POMs or broken templates,
 * not actual project repos worth querying.
 */
function isJunkRepoUrl(repoUrl: string): boolean {
  return (
    repoUrl.includes('sonatype/jvnet-parent') || repoUrl.includes('${') // broken URL templates
  )
}

/**
 * Last-resort heuristic for org.apache.* packages when SCM URL extraction fails.
 * Apache mirrors all its projects to github.com/apache/ using the project name.
 * Derives the project name from the groupId sub-namespace (e.g. org.apache.kafka → kafka).
 * Returns null for non-Apache groupIds.
 */
function guessApacheGithubUrl(groupId: string, artifactId: string): string | null {
  if (!groupId.startsWith('org.apache') && !groupId.startsWith('commons-')) return null

  if (groupId.startsWith('org.apache.')) {
    const sub = groupId.slice('org.apache.'.length)
    const firstSegment = sub.split('.')[0]

    // org.apache.commons → commons projects follow apache/commons-{artifactId} convention
    // e.g. org.apache.commons + commons-io → apache/commons-io
    if (firstSegment === 'commons') {
      return `https://github.com/apache/${artifactId}`
    }

    // org.apache.tomcat.embed → tomcat (use first segment)
    return `https://github.com/apache/${firstSegment}`
  }

  if (groupId === 'org.apache') {
    // Bare org.apache — use artifactId as-is
    return `https://github.com/apache/${artifactId}`
  }

  if (groupId.startsWith('commons-')) {
    // commons-collections:commons-collections → apache/commons-collections
    return `https://github.com/apache/${groupId}`
  }

  return null
}

function parseGithubRepo(url: string): { owner: string; repo: string } | null {
  const resolved = toGithubUrl(url)
  const m = resolved.match(/github\.com\/([^/]+)\/([^/]+?)(?:\.git)?(?:\/.*)?$/)
  if (!m) return null
  return { owner: m[1], repo: m[2] }
}

// ─── GitHub: CODEOWNERS ───────────────────────────────────────────────────────

const CODEOWNERS_PATHS = ['CODEOWNERS', '.github/CODEOWNERS', 'docs/CODEOWNERS']

async function fetchCODEOWNERS(owner: string, repo: string): Promise<string | null> {
  for (const p of CODEOWNERS_PATHS) {
    const data = await safeGet<{ content?: string }>(
      github,
      `/repos/${owner}/${repo}/contents/${p}`,
    )
    if (data?.content) {
      return Buffer.from(data.content, 'base64').toString('utf-8')
    }
  }
  return null
}

// Extract @username handles from CODEOWNERS (skip @org/team entries)
function parseCODEOWNERS(content: string): string[] {
  const logins: string[] = []
  const seen = new Set<string>()
  for (const line of content.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    // Match @login — skip @org/team entries (slash immediately after the handle)
    let match: RegExpExecArray | null
    const re = /@([a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)/g
    while ((match = re.exec(trimmed)) !== null) {
      if (trimmed[match.index + match[0].length] === '/') continue
      const login = match[1].toLowerCase()
      if (!seen.has(login)) {
        seen.add(login)
        logins.push(login)
      }
    }
  }
  return logins
}

// ─── GitHub: MAINTAINERS file ─────────────────────────────────────────────────

const MAINTAINERS_PATHS = [
  'MAINTAINERS.md',
  'MAINTAINERS',
  'MAINTAINERS.txt',
  'docs/MAINTAINERS.md',
]

async function fetchMAINTAINERS(owner: string, repo: string): Promise<string | null> {
  for (const p of MAINTAINERS_PATHS) {
    const data = await safeGet<{ content?: string }>(
      github,
      `/repos/${owner}/${repo}/contents/${p}`,
    )
    if (data?.content) {
      return Buffer.from(data.content, 'base64').toString('utf-8')
    }
  }
  return null
}

interface ParsedMaintainer {
  login: string
  displayName: string
  email: string
}

// Extract GitHub handles and emails from free-form MAINTAINERS file
function parseMAINTAINERS(content: string): ParsedMaintainer[] {
  const results: ParsedMaintainer[] = []
  const seenLogins = new Set<string>()

  for (const line of content.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('|--')) continue

    // Try to extract @login
    const loginMatch = trimmed.match(/@([a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)/)
    const login = loginMatch ? loginMatch[1].toLowerCase() : ''

    // Try to extract email
    const emailMatch = trimmed.match(/[\w.+-]+@[\w-]+\.[\w.]+/)
    const email = emailMatch ? emailMatch[0] : ''

    // Try to extract display name — text before the @handle or email
    const nameMatch = trimmed.match(/^[*\-\s]*([A-Z][^@<|(]+?)(?:\s*[@<|(]|$)/)
    const displayName = nameMatch ? nameMatch[1].trim() : ''

    if (!login && !email) continue
    if (login && seenLogins.has(login)) continue
    if (login) seenLogins.add(login)

    results.push({ login, displayName, email })
  }

  return results
}

// ─── GitHub: contributors fallback ───────────────────────────────────────────

interface GithubContributor {
  login: string
  html_url: string
  contributions: number
  type: string
}

async function fetchTopContributors(
  owner: string,
  repo: string,
  limit: number,
): Promise<GithubContributor[]> {
  const results: GithubContributor[] = []
  let page = 1
  let hasMore = true

  while (hasMore) {
    const data = await safeGet<GithubContributor[]>(
      github,
      `/repos/${owner}/${repo}/contributors?per_page=100&page=${page}&anon=false`,
    )
    if (!data || data.length === 0) break

    const users = data.filter((c) => c.type === 'User')
    results.push(...users)

    hasMore = data.length === 100 && (limit === 0 || results.length < limit)
    if (hasMore) {
      page++
      await sleep(GITHUB_DELAY_MS)
    }
  }

  return limit > 0 ? results.slice(0, limit) : results
}

// ─── Concurrency pool ─────────────────────────────────────────────────────────

async function withConcurrency<T>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<void>,
): Promise<void> {
  let index = 0
  async function worker(): Promise<void> {
    while (index < items.length) {
      const i = index++
      await fn(items[i], i)
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker))
}

// ─── Per-package enrichment ───────────────────────────────────────────────────

async function enrichPackage(row: Record<string, string>): Promise<MaintainerRow[]> {
  const { purl, namespace: groupId, name: artifactId } = row

  const base: Omit<
    MaintainerRow,
    | 'maintainer_github_login'
    | 'maintainer_display_name'
    | 'maintainer_email'
    | 'maintainer_url'
    | 'role'
    | 'source'
    | 'contributions'
    | 'notes'
    | 'github_repo'
    | 'repo_source'
  > = {
    package_purl: purl,
    package_namespace: groupId,
    package_name: artifactId,
  }

  const emptyMaintainer = (): MaintainerRow => ({
    ...base,
    github_repo: '',
    repo_source: '',
    maintainer_github_login: '',
    maintainer_display_name: '',
    maintainer_email: '',
    maintainer_url: '',
    role: '',
    source: '',
    contributions: '',
    notes: 'no maintainer found',
  })

  // 1a. Check hardcoded overrides first (fixes known wrong Libraries.io mappings)
  const overrideUrl = getRepoOverride(groupId)

  // 1b. Resolve GitHub repo URL via Libraries.io (skip if junk)
  const libIoUrl = await getLibIoRepoUrl(groupId, artifactId)
  await sleep(LIBRARIES_IO_DELAY_MS)
  const validLibIoUrl = libIoUrl && !isJunkRepoUrl(libIoUrl) ? libIoUrl : null

  // 1c. Fallback: extract SCM URL from Maven Central POM
  let repoUrl = overrideUrl ?? validLibIoUrl
  let repoSource = overrideUrl ? 'override' : validLibIoUrl ? 'libraries.io' : ''
  if (!repoUrl) {
    repoUrl = await getPomScmUrl(groupId, artifactId)
    repoSource = 'pom_scm'
  }

  // 1d. Last-resort: Apache namespace heuristic (org.apache.* → github.com/apache/*)
  if (!repoUrl || !parseGithubRepo(repoUrl)) {
    const guess = guessApacheGithubUrl(groupId, artifactId)
    if (guess) {
      repoUrl = guess
      repoSource = 'apache_heuristic'
    }
  }

  if (!repoUrl) {
    process.stdout.write(`(no repo url) `)
    return [{ ...emptyMaintainer(), notes: 'no github repo found' }]
  }

  const parsed = parseGithubRepo(repoUrl)
  if (!parsed) {
    process.stdout.write(`(non-github: ${repoUrl}) `)
    return [{ ...emptyMaintainer(), notes: `non-github repo [${repoSource}]: ${repoUrl}` }]
  }

  const { owner, repo } = parsed
  const github_repo = `${owner}/${repo}`

  // 2. Try CODEOWNERS
  const codeownersContent = await fetchCODEOWNERS(owner, repo)
  await sleep(GITHUB_DELAY_MS)

  if (codeownersContent) {
    const logins = parseCODEOWNERS(codeownersContent)
    if (logins.length > 0) {
      return logins.map((login) => ({
        ...base,
        github_repo,
        repo_source: repoSource,
        maintainer_github_login: login,
        maintainer_display_name: '',
        maintainer_email: '',
        maintainer_url: `https://github.com/${login}`,
        role: 'maintainer',
        source: 'codeowners',
        contributions: '',
        notes: '',
      }))
    }
  }

  // 3. Try MAINTAINERS file
  const maintainersContent = await fetchMAINTAINERS(owner, repo)
  await sleep(GITHUB_DELAY_MS)

  if (maintainersContent) {
    const maintainers = parseMAINTAINERS(maintainersContent)
    if (maintainers.length > 0) {
      return maintainers.map((m) => ({
        ...base,
        github_repo,
        repo_source: repoSource,
        maintainer_github_login: m.login,
        maintainer_display_name: m.displayName,
        maintainer_email: m.email,
        maintainer_url: m.login ? `https://github.com/${m.login}` : '',
        role: 'maintainer',
        source: 'maintainers_file',
        contributions: '',
        notes: '',
      }))
    }
  }

  // 4. Fallback: top contributors
  const contributors = await fetchTopContributors(owner, repo, TOP_CONTRIBUTORS_LIMIT)
  await sleep(GITHUB_DELAY_MS)

  if (contributors.length > 0) {
    return contributors.map((c) => ({
      ...base,
      github_repo,
      repo_source: repoSource,
      maintainer_github_login: c.login,
      maintainer_display_name: '',
      maintainer_email: '',
      maintainer_url: c.html_url,
      role: 'contributor',
      source: 'github_contributors',
      contributions: String(c.contributions),
      notes:
        TOP_CONTRIBUTORS_LIMIT > 0
          ? `top ${TOP_CONTRIBUTORS_LIMIT} contributors`
          : 'all contributors',
    }))
  }

  return [
    {
      ...emptyMaintainer(),
      github_repo,
      repo_source: repoSource,
      notes: 'repo found but no maintainer data',
    },
  ]
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  if (!LIBRARIES_IO_KEY) {
    console.error('Missing LIBRARIES_IO_API_KEY env var')
    process.exit(1)
  }
  if (!GITHUB_TOKEN) {
    console.warn(
      'Warning: GITHUB_TOKEN not set — GitHub API rate limit will be very low (60 req/hr)',
    )
  }

  const inputPath = process.argv[2]
  if (!inputPath) {
    console.error(
      'Usage: LIBRARIES_IO_API_KEY=xxx GITHUB_TOKEN=yyy tsx src/maven/scripts/fetchMavenMaintainers.ts <input.csv> [output.csv]',
    )
    process.exit(1)
  }

  const outputPath =
    process.argv[3] ??
    path.join(path.dirname(inputPath), path.basename(inputPath, '.csv') + '_maintainers.csv')

  const content = fs.readFileSync(inputPath, 'utf-8')
  const rows = parseCsv(content).filter((r) => r.ecosystem === 'maven' && r.purl)

  console.log(`Found ${rows.length} maven packages — output: ${outputPath}`)
  if (!GITHUB_TOKEN) console.warn('  (no GitHub token — using unauthenticated API, very slow)')

  const CSV_HEADER =
    'package_purl,package_namespace,package_name,github_repo,repo_source,maintainer_github_login,maintainer_display_name,maintainer_email,maintainer_url,role,source,contributions,notes'

  const resultsByIndex: MaintainerRow[][] = new Array(rows.length)
  const stats = { codeowners: 0, maintainers_file: 0, contributors: 0, none: 0 }
  let completed = 0

  // Concurrency 3 — Libraries.io rate limit is the bottleneck
  await withConcurrency(rows, 3, async (row, i) => {
    const pkg = `${row.namespace}/${row.name}`
    process.stdout.write(`[${i + 1}/${rows.length}] ${pkg} ... `)

    const results = await enrichPackage(row)
    resultsByIndex[i] = results

    const source = results[0]?.source ?? ''
    if (source === 'codeowners') stats.codeowners++
    else if (source === 'maintainers_file') stats.maintainers_file++
    else if (source === 'github_contributors') stats.contributors++
    else stats.none++

    console.log(`${results.length} maintainer(s) [${source || 'none'}]`)

    // Checkpoint every 50 packages — write rows in original input order
    completed++
    if (completed % 50 === 0) {
      const checkpointLines = [CSV_HEADER, ...resultsByIndex.flat().map(toCsvLine)]
      fs.writeFileSync(outputPath, checkpointLines.join('\n'))
      console.log(`  ✓ checkpoint saved (${completed} done)`)
    }
  })

  const lines = [CSV_HEADER, ...resultsByIndex.flat().map(toCsvLine)]
  fs.writeFileSync(outputPath, lines.join('\n'))
  console.log(`\nDone. ${lines.length - 1} rows written to: ${outputPath}`)

  console.log(
    `  CODEOWNERS: ${stats.codeowners} | MAINTAINERS file: ${stats.maintainers_file} | contributors fallback: ${stats.contributors} | not found: ${stats.none}`,
  )
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
