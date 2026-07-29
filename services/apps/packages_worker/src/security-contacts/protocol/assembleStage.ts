import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { prepareBulkInsert } from '@crowd/data-access-layer/src/utils'
import { getServiceChildLogger } from '@crowd/logging'

import { getReportingProtocolConfig } from '../../config'

import { AssembleInput, assembleProtocol } from './assemble'
import { AssembleStageResult } from './types'

const log = getServiceChildLogger('reporting-protocol:assemble')

type Cfg = ReturnType<typeof getReportingProtocolConfig>

const PROTOCOL_COLUMNS = ['repo_id', 'declared', 'methods', 'guidelines', 'sources']
const PROTOCOL_UPSERT_SET = `(repo_id) DO UPDATE SET
   declared = EXCLUDED.declared,
   methods = EXCLUDED.methods,
   guidelines = EXCLUDED.guidelines,
   sources = EXCLUDED.sources,
   assembled_at = NOW()`

interface RepoRow {
  id: string
  url: string
  pvr_enabled: boolean | null
  security_txt_url: string | null
  file_parses: AssembleInput['fileParses'] | null
  page_parses: AssembleInput['pageParses'] | null
  fallback_contacts: AssembleInput['fallbackContacts'] | null
}

async function selectReposToAssemble(qx: QueryExecutor, limit: number): Promise<RepoRow[]> {
  return qx.select(
    `WITH stale AS (
       SELECT DISTINCT r.id
       FROM repos r
       JOIN package_repos pr ON pr.repo_id = r.id
       JOIN packages p ON p.id = pr.package_id AND p.is_critical
       LEFT JOIN repo_reporting_protocols rp ON rp.repo_id = r.id
       WHERE rp.repo_id IS NULL
          OR r.contacts_last_refreshed > rp.assembled_at
          OR EXISTS (
               SELECT 1 FROM repo_well_known_files w
               JOIN security_policy_parses sp ON sp.blob_oid = w.blob_oid
               WHERE w.repo_id = r.id AND w.file_type = 'security'
                 AND w.deleted_at IS NULL AND sp.parsed_at > rp.assembled_at)
       LIMIT $(limit)
     )
     SELECT r.id::text AS id, r.url, r.pvr_enabled, r.security_txt_url,
       (SELECT json_agg(json_build_object(
            'blobOid', sp.blob_oid, 'path', w.path, 'parser', sp.parser,
            'status', sp.status, 'parsed', sp.parsed))
          FROM repo_well_known_files w
          JOIN security_policy_parses sp ON sp.blob_oid = w.blob_oid
         WHERE w.repo_id = r.id AND w.file_type = 'security' AND w.deleted_at IS NULL
       ) AS file_parses,
       (SELECT json_agg(json_build_object(
            'hash', lp.blob_oid, 'url', lp.url, 'parser', lp.parser,
            'status', lp.status, 'parsed', lp.parsed))
          FROM repo_well_known_files w
          JOIN security_policy_parses sp ON sp.blob_oid = w.blob_oid
          JOIN security_policy_parses lp
            ON lp.source_kind = 'linked-page' AND lp.url = ANY(sp.linked_urls)
         WHERE w.repo_id = r.id AND w.file_type = 'security' AND w.deleted_at IS NULL
       ) AS page_parses,
       (SELECT json_agg(json_build_object(
            'channel', sc.channel, 'value', sc.value, 'score', sc.score))
          FROM security_contacts sc
         WHERE sc.repo_id = r.id AND sc.deleted_at IS NULL
       ) AS fallback_contacts
     FROM repos r
     JOIN stale s ON s.id = r.id`,
    { limit },
  )
}

export async function runAssembleStage(qx: QueryExecutor, cfg: Cfg): Promise<AssembleStageResult> {
  const repos = await selectReposToAssemble(qx, cfg.assembleBatchSize)
  const rows = repos.map((r) => {
    const assembled = assembleProtocol({
      repoUrl: r.url,
      pvrEnabled: r.pvr_enabled,
      securityTxtUrl: r.security_txt_url,
      fileParses: r.file_parses ?? [],
      pageParses: r.page_parses ?? [],
      fallbackContacts: (r.fallback_contacts ?? []).map((c) => ({ ...c, score: Number(c.score) })),
    })
    return {
      repo_id: r.id,
      declared: assembled.declared,
      methods: JSON.stringify(assembled.methods),
      guidelines: assembled.guidelines ? JSON.stringify(assembled.guidelines) : null,
      sources: JSON.stringify(assembled.sources),
    }
  })

  if (rows.length > 0) {
    await qx.result(
      prepareBulkInsert('repo_reporting_protocols', PROTOCOL_COLUMNS, rows, PROTOCOL_UPSERT_SET),
    )
  }

  const result: AssembleStageResult = {
    reposAssembled: rows.length,
    declaredCount: rows.filter((r) => r.declared).length,
  }
  log.info({ ...result }, 'Reporting protocol assemble stage complete')
  return result
}
