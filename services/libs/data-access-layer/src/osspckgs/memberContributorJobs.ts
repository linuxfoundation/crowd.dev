import { QueryExecutor } from '../queryExecutor'

export type MemberContributorCollectionPath = 'github' | 'llm_repo_resolution'
export type MemberContributorJobStatus = 'pending' | 'running' | 'done' | 'failed' | 'unresolvable'
export type MemberContributorRepoUrlSource = 'llm_inferred'

export interface MemberContributorJobInput {
  id: string
  purl: string
  ecosystem: string
  collectionPath: MemberContributorCollectionPath
  packageId: string | null
  repoId: string | null
}

export interface MemberContributorJobRow {
  id: string
  purl: string
  ecosystem: string
  collectionPath: MemberContributorCollectionPath
  status: MemberContributorJobStatus
  error: string | null
  packageId: string | null
  repoId: string | null
  repoUrl: string | null
  repoUrlSource: MemberContributorRepoUrlSource | null
}

export interface MemberContributorJobPurl {
  purl: string
  ecosystem: string
}

export interface LatestMemberContributorJob {
  id: string
  status: MemberContributorJobStatus
  collectionPath: MemberContributorCollectionPath
}

export interface LlmUsage {
  inputTokens: number
  outputTokens: number
  costUsd: number | null
}

export interface InferredRepoRecord {
  repoUrl: string
  confidence: number
}

export interface MemberContributorJobRepo {
  repoId: string
  repoUrl: string
  repoHost: string
}

export async function createMemberContributorJob(
  qx: QueryExecutor,
  input: MemberContributorJobInput,
): Promise<string | null> {
  const row = await qx.selectOneOrNone(
    `
    INSERT INTO member_contributor_jobs
      (id, purl, ecosystem, collection_path, package_id, repo_id, status)
    VALUES
      ($(id), $(purl), $(ecosystem), $(collectionPath), $(packageId), $(repoId), 'pending')
    ON CONFLICT (purl) WHERE status IN ('pending', 'running') DO NOTHING
    RETURNING id
    `,
    input,
  )
  return row ? (row.id as string) : null
}

export async function findActiveMemberContributorJobByPurl(
  qx: QueryExecutor,
  purl: string,
): Promise<{ id: string } | null> {
  return qx.selectOneOrNone(
    `
    SELECT id
    FROM member_contributor_jobs
    WHERE purl = $(purl) AND status IN ('pending', 'running')
    `,
    { purl },
  )
}

export async function findLatestMemberContributorJobStatusByPurl(
  qx: QueryExecutor,
  purl: string,
): Promise<MemberContributorJobStatus | null> {
  const row = await qx.selectOneOrNone(
    `
    SELECT status
    FROM member_contributor_jobs
    WHERE purl = $(purl)
    ORDER BY created_at DESC, id DESC
    LIMIT 1
    `,
    { purl },
  )
  return row ? (row.status as MemberContributorJobStatus) : null
}

export async function findMemberContributorJob(
  qx: QueryExecutor,
  id: string,
): Promise<MemberContributorJobRow | null> {
  return qx.selectOneOrNone(
    `
    SELECT id, purl, ecosystem, status, error,
           collection_path AS "collectionPath",
           package_id::text AS "packageId", repo_id::text AS "repoId",
           repo_url AS "repoUrl", repo_url_source AS "repoUrlSource"
    FROM member_contributor_jobs
    WHERE id = $(id)
    `,
    { id },
  )
}

export async function findMemberContributorJobRepo(
  qx: QueryExecutor,
  id: string,
): Promise<MemberContributorJobRepo | null> {
  return qx.selectOneOrNone(
    `
    SELECT j.repo_id::text AS "repoId", r.url AS "repoUrl", r.host AS "repoHost"
    FROM member_contributor_jobs j
    JOIN repos r ON r.id = j.repo_id
    WHERE j.id = $(id)
    `,
    { id },
  )
}

export async function markMemberContributorJobRunning(
  qx: QueryExecutor,
  id: string,
): Promise<void> {
  await qx.result(
    `
    UPDATE member_contributor_jobs
    SET status = 'running', started_at = NOW(), updated_at = NOW()
    WHERE id = $(id) AND status = 'pending'
    `,
    { id },
  )
}

export async function completeMemberContributorJob(qx: QueryExecutor, id: string): Promise<void> {
  await qx.result(
    `
    UPDATE member_contributor_jobs
    SET status = 'done', completed_at = NOW(), updated_at = NOW()
    WHERE id = $(id) AND status = 'running'
    `,
    { id },
  )
}

export async function failMemberContributorJob(
  qx: QueryExecutor,
  id: string,
  error: string,
): Promise<void> {
  await qx.result(
    `
    UPDATE member_contributor_jobs
    SET status = 'failed', error = $(error), completed_at = NOW(), updated_at = NOW()
    WHERE id = $(id) AND status <> 'done'
    `,
    { id, error },
  )
}

export async function findMemberContributorJobPurl(
  qx: QueryExecutor,
  id: string,
): Promise<MemberContributorJobPurl | null> {
  return qx.selectOneOrNone(
    `SELECT purl, ecosystem FROM member_contributor_jobs WHERE id = $(id)`,
    {
      id,
    },
  )
}

export async function findLatestMemberContributorJobByPurl(
  qx: QueryExecutor,
  purl: string,
): Promise<LatestMemberContributorJob | null> {
  return qx.selectOneOrNone(
    `
    SELECT id, status, collection_path AS "collectionPath"
    FROM member_contributor_jobs
    WHERE purl = $(purl)
    ORDER BY created_at DESC, id DESC
    LIMIT 1
    `,
    { purl },
  )
}

export async function recordMemberContributorJobInferredRepo(
  qx: QueryExecutor,
  id: string,
  record: InferredRepoRecord,
): Promise<void> {
  await qx.result(
    `
    UPDATE member_contributor_jobs
    SET repo_url = $(repoUrl), repo_url_source = 'llm_inferred', llm_confidence = $(confidence),
        updated_at = NOW()
    WHERE id = $(id) AND status = 'running'
    `,
    { id, ...record },
  )
}

export async function recordMemberContributorJobLlmUsage(
  qx: QueryExecutor,
  id: string,
  usage: LlmUsage,
): Promise<void> {
  await qx.result(
    `
    UPDATE member_contributor_jobs
    SET llm_input_tokens = COALESCE(llm_input_tokens, 0) + $(inputTokens),
        llm_output_tokens = COALESCE(llm_output_tokens, 0) + $(outputTokens),
        llm_cost_usd = CASE WHEN $(costUsd) IS NULL THEN llm_cost_usd ELSE COALESCE(llm_cost_usd, 0) + $(costUsd) END,
        updated_at = NOW()
    WHERE id = $(id) AND status = 'running'
    `,
    { id, ...usage },
  )
}

export async function markMemberContributorJobUnresolvable(
  qx: QueryExecutor,
  id: string,
  reason: string,
  repoUrl: string | null,
): Promise<void> {
  await qx.result(
    `
    UPDATE member_contributor_jobs
    SET status = 'unresolvable', error = $(reason), repo_url = COALESCE($(repoUrl), repo_url),
        completed_at = NOW(), updated_at = NOW()
    WHERE id = $(id) AND status <> 'done'
    `,
    { id, reason, repoUrl },
  )
}

export async function incrementMemberContributorJobSubmissions(
  qx: QueryExecutor,
  id: string,
): Promise<void> {
  await qx.result(
    `UPDATE member_contributor_jobs SET submission_count = submission_count + 1, updated_at = NOW() WHERE id = $(id)`,
    { id },
  )
}
