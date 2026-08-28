import { getServiceChildLogger } from '@crowd/logging'

import { getCdpDb, getPackagesDb } from '../db'
import { buildInsert } from '../deps-dev/sqlUtils'

const log = getServiceChildLogger('syncLfGithubRepos')

export async function syncLfGithubRepos(): Promise<{ inserted: number }> {
  const cdpDb = await getCdpDb()
  const pkgsDb = await getPackagesDb()

  const cdpRows = (await cdpDb.select(
    `
    SELECT LOWER(r.url) AS url
    FROM public.repositories r
    JOIN public.integrations i ON r."sourceIntegrationId" = i.id
    JOIN public."insightsProjects" ip ON r."insightsProjectId" = ip.id
    WHERE ip."isLF" = true
      AND i.platform IN ('github', 'github-nango')
      AND r."deletedAt" IS NULL
      AND r.archived = false
      AND r.excluded = false
    `,
  )) as Array<{ url: string }>

  if (cdpRows.length === 0) {
    log.info('No LF GitHub repos found in CDP db')
    return { inserted: 0 }
  }

  log.info({ count: cdpRows.length }, 'Seeding LF GitHub repos into packages-db')

  type RepoRow = { url: string; host: string; owner: string; name: string }

  const rows: RepoRow[] = []
  for (const { url } of cdpRows) {
    const path = url.replace('https://github.com/', '')
    const slash = path.indexOf('/')
    if (slash <= 0 || slash === path.length - 1) continue
    rows.push({
      url,
      host: 'github',
      owner: path.slice(0, slash),
      name: path.slice(slash + 1),
    })
  }

  if (rows.length === 0) {
    log.warn('All CDP rows failed URL parsing — no repos seeded')
    return { inserted: 0 }
  }

  const sql =
    buildInsert('repos', ['url', 'host', 'owner', 'name'], rows) + '\nON CONFLICT (url) DO NOTHING'

  const inserted = await pkgsDb.result(sql)
  log.info({ inserted }, 'LF GitHub repos seeded into packages-db')
  return { inserted }
}
