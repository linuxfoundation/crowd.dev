import CronTime from 'cron-time-generator'
import yaml from 'js-yaml'

import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getRepositoriesByUrl } from '@crowd/data-access-layer/src/repositories'
import {
  createRepositoryGroup,
  listRepositoryGroups,
  updateRepositoryGroup,
} from '@crowd/data-access-layer/src/repositoryGroups'

import { IJobDefinition } from '../types'

// ---------------------------------------------------------------------------
// Config — one entry per governance YAML source we want to sync.
// Adding a second platform in the future is as simple as appending an entry.
// ---------------------------------------------------------------------------
const GOVERNANCE_SOURCES = [
  {
    // OpenStack governance YAML published by the TC
    yamlUrl: 'https://opendev.org/openstack/governance/raw/branch/master/reference/projects.yaml',
    // Repos in the YAML are listed as "<owner>/<repo>".
    // We convert them to full URLs using this prefix.
    repoUrlBase: 'https://review.opendev.org/',
    // ID of the insightsProject that owns these repository groups.
    insightsProjectSlug: 'OpenStack',
  },
]

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
type GovernanceYaml = Record<
  string,
  {
    deliverables?: Record<string, { repos?: string[] }>
  }
>

interface ParsedProject {
  project: string
  repos: string[]
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
async function fetchProjects(yamlUrl: string): Promise<ParsedProject[]> {
  let text: string

  try {
    const response = await fetch(yamlUrl)

    if (!response.ok) {
      throw new Error(`HTTP ${response.status} ${response.statusText}`)
    }
    text = await response.text()
  } catch (err) {
    throw new Error(`Failed to fetch governance YAML from ${yamlUrl}: ${(err as Error).message}`)
  }

  let data: GovernanceYaml
  try {
    data = yaml.load(text) as GovernanceYaml
    if (!data || typeof data !== 'object') {
      throw new Error('Parsed YAML is not an object — file format may have changed')
    }
  } catch (err) {
    throw new Error(`Failed to parse governance YAML from ${yamlUrl}: ${(err as Error).message}`)
  }

  return Object.entries(data).map(([project, info]) => ({
    project,
    repos: Object.values(info?.deliverables ?? {}).flatMap((d) => d.repos ?? []),
  }))
}

function toSlug(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
}

// ---------------------------------------------------------------------------
// Job definition
// ---------------------------------------------------------------------------
const job: IJobDefinition = {
  name: 'openstack-repository-groups-sync',
  // Run once a week
  cronTime: CronTime.everyWeek(),
  timeout: 30 * 60, // 30 minutes

  process: async (ctx) => {
    ctx.log.info('Starting OpenStack repository groups sync...')

    const dbConnection = await getDbConnection(WRITE_DB_CONFIG(), 3, 0)
    const qx = pgpQx(dbConnection)

    for (const source of GOVERNANCE_SOURCES) {
      ctx.log.debug(`Processing source: ${source.yamlUrl}`)

      // ------------------------------------------------------------------
      // 1. Resolve the insights project
      // ------------------------------------------------------------------
      const insightsProject = await qx.selectOneOrNone(
        `SELECT id FROM "insightsProjects" WHERE slug = $(slug) AND "deletedAt" IS NULL`,
        { slug: source.insightsProjectSlug },
      )

      if (!insightsProject) {
        ctx.log.warn(
          `Insights project with slug '${source.insightsProjectSlug}' not found — skipping source.`,
        )
        continue
      }

      const insightsProjectId: string = insightsProject.id
      ctx.log.debug(`Resolved insights project: ${insightsProjectId}`)

      // ------------------------------------------------------------------
      // 2. Fetch + parse the governance YAML
      // ------------------------------------------------------------------
      ctx.log.debug(`Fetching governance YAML...`)
      let projects: ParsedProject[]

      try {
        projects = await fetchProjects(source.yamlUrl)
      } catch (err) {
        ctx.log.error({ err }, `Could not load governance YAML — skipping source`)
        continue
      }

      ctx.log.info(`Parsed ${projects.length} projects from YAML`)

      // ------------------------------------------------------------------
      // 3. Load existing repository groups so we can upsert
      // ------------------------------------------------------------------
      const existingGroups = await listRepositoryGroups(qx, { insightsProjectId })
      const existingBySlug = new Map(existingGroups.map((g) => [g.slug, g]))

      let created = 0
      let updated = 0
      let skipped = 0

      // ------------------------------------------------------------------
      // 4. Bulk-fetch all repo URLs that exist in the DB (single round-trip)
      // ------------------------------------------------------------------
      const allCandidateUrls = projects.flatMap(({ repos }) =>
        repos.map((r) => `${source.repoUrlBase}${r}`),
      )
      const foundRepos = await getRepositoriesByUrl(qx, allCandidateUrls)
      const foundUrlSet = new Set(foundRepos.map((r) => r.url))

      // ------------------------------------------------------------------
      // 5. Upsert one repository group per YAML project
      // ------------------------------------------------------------------
      for (const { project, repos } of projects) {
        if (repos.length === 0) {
          ctx.log.debug(`'${project}' has no repos in YAML — skipping`)
          skipped++
          continue
        }

        const slug = toSlug(project)
        const candidateUrls = repos.map((r) => `${source.repoUrlBase}${r}`)
        const foundUrls = candidateUrls.filter((u) => foundUrlSet.has(u))

        if (foundUrls.length === 0) {
          ctx.log.debug(
            `'${project}': none of the ${candidateUrls.length} repo URLs exist in the repositories table — skipping`,
          )
          skipped++
          continue
        }

        const missing = candidateUrls.filter((u) => !foundUrlSet.has(u))
        if (missing.length > 0) {
          ctx.log.warn(
            `'${project}': ${missing.length}/${candidateUrls.length} repos not found in DB` +
              ` (first 5: ${missing.slice(0, 5).join(', ')}${missing.length > 5 ? '...' : ''})`,
          )
        }

        const existingGroup = existingBySlug.get(slug)

        if (existingGroup) {
          await updateRepositoryGroup(qx, existingGroup.id, {
            name: project,
            slug,
            repositories: foundUrls,
          })
          ctx.log.info(`Updated '${project}' — ${foundUrls.length} repos`)
          updated++
        } else {
          await createRepositoryGroup(qx, {
            name: project,
            slug,
            insightsProjectId,
            repositories: foundUrls,
          })
          ctx.log.info(`Created '${project}' — ${foundUrls.length} repos`)
          created++
        }
      }

      ctx.log.debug(`Source done — created: ${created}, updated: ${updated}, skipped: ${skipped}`)
    }

    ctx.log.info('OpenStack repository groups sync complete')
  },
}

export default job
