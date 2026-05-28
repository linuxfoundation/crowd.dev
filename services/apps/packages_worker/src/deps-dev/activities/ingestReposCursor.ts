import { createIngestJob, markJobStatus } from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getPackagesDb } from '../../db'
import { bigquery } from '../config'
import { canonicalRepoUrl, parseRepoUrl } from '../canonicalRepoUrl'
import { formatValue } from '../sqlUtils'

const log = getServiceChildLogger('ingestReposCursor')

const BATCH_SIZE = 5_000

const REPOS_SQL = `
SELECT Type, Name, OpenIssuesCount, StarsCount, ForksCount, Description, Homepage
FROM \`bigquery-public-data.deps_dev_v1.ProjectsLatest\`
WHERE Type IN ('GITHUB', 'GITLAB', 'BITBUCKET')
`

interface BqRepoRow {
  Type: string
  Name: string
  OpenIssuesCount: number | null
  StarsCount: number | null
  ForksCount: number | null
  Description: string | null
  Homepage: string | null
}

function buildReposBatchInsert(rows: Array<BqRepoRow & { canonicalUrl: string }>): string {
  const values = rows
    .map((r) => {
      const url = r.canonicalUrl
      const parsed = parseRepoUrl(url)
      return [
        formatValue(url),
        formatValue(r.Type),
        formatValue(r.Name),
        formatValue(parsed.host),
        formatValue(parsed.owner),
        formatValue(parsed.name),
        formatValue(r.Description),
        formatValue(r.Homepage),
        formatValue(r.StarsCount),
        formatValue(r.ForksCount),
        formatValue(r.OpenIssuesCount),
        'NOW()',
      ].join(', ')
    })
    .map((v) => `(${v})`)
    .join(',\n')

  return `
INSERT INTO repos (url, raw_project_type, raw_project_name, host, owner, name,
                   description, homepage, stars, forks, open_issues, last_synced_at)
VALUES
${values}
ON CONFLICT (url) DO UPDATE SET
  description      = EXCLUDED.description,
  homepage         = EXCLUDED.homepage,
  stars            = EXCLUDED.stars,
  forks            = EXCLUDED.forks,
  open_issues      = EXCLUDED.open_issues,
  raw_project_type = EXCLUDED.raw_project_type,
  raw_project_name = EXCLUDED.raw_project_name,
  last_synced_at   = NOW()
`
}

export interface IngestReposCursorInput {
  runId: string
}

export interface IngestReposCursorOutput {
  rowsInserted: number
}

export async function ingestReposCursor(
  _input: IngestReposCursorInput,
): Promise<IngestReposCursorOutput> {
  const qx = await getPackagesDb()
  // M8: cursor job — provisional watermark is today's date (monotonic, not a partition date)
  const jobId = await createIngestJob(qx, 'repos', 'full', new Date())
  await markJobStatus(qx, jobId, 'loading')

  const stream = bigquery.createQueryStream({ query: REPOS_SQL })

  let batch: Array<BqRepoRow & { canonicalUrl: string }> = []
  let totalInserted = 0
  let skipped = 0

  for await (const row of stream as AsyncIterable<BqRepoRow>) {
    const canonicalUrl = canonicalRepoUrl(row.Type, row.Name)
    if (!canonicalUrl) {
      skipped++
      continue
    }
    batch.push({ ...row, canonicalUrl })
    if (batch.length >= BATCH_SIZE) {
      await qx.result(buildReposBatchInsert(batch))
      totalInserted += batch.length
      batch = []
      log.info({ jobId, totalInserted }, 'repos cursor progress')
    }
  }

  if (batch.length > 0) {
    await qx.result(buildReposBatchInsert(batch))
    totalInserted += batch.length
  }

  if (skipped > 0) {
    log.warn({ skipped }, 'Skipped repos with malformed URL')
  }

  await markJobStatus(qx, jobId, 'done', { finishedAt: new Date(), rowCountPg: totalInserted })
  log.info({ jobId, totalInserted }, 'repos cursor complete')

  return { rowsInserted: totalInserted }
}
