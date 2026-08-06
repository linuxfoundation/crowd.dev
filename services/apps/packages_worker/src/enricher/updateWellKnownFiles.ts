import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { RepoWellKnownFilesUpdate } from './types'

export async function bulkUpsertRepoWellKnownFiles(
  qx: QueryExecutor,
  updates: RepoWellKnownFilesUpdate[],
): Promise<void> {
  if (updates.length === 0) return

  // dedupe by repoId: ON CONFLICT DO UPDATE cannot affect the same row twice in one statement
  const byRepo = new Map(updates.map((u) => [u.repoId, u]))
  const batch = [...byRepo.values()]

  await qx.result(
    `
    WITH input AS (
      SELECT
        (j->>'repoId')::bigint        AS repo_id,
        (j->>'checkedAt')::timestamptz AS checked_at,
        j->'files'                     AS files
      FROM jsonb_array_elements($1::jsonb) j
    ),
    found AS (
      SELECT
        i.repo_id,
        f->>'fileType'  AS file_type,
        f->>'directory' AS directory,
        f->>'path'      AS path,
        f->>'blobOid'   AS blob_oid,
        i.checked_at
      FROM input i, jsonb_array_elements(i.files) f
    ),
    soft_deleted AS (
      UPDATE repo_well_known_files w
      SET deleted_at = i.checked_at,
          change_detected_at = i.checked_at
      FROM input i
      WHERE w.repo_id = i.repo_id
        AND w.deleted_at IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM found f WHERE f.repo_id = w.repo_id AND f.path = w.path
        )
    )
    INSERT INTO repo_well_known_files (repo_id, file_type, directory, path, blob_oid, checked_at, change_detected_at)
    SELECT repo_id, file_type, directory, path, blob_oid, checked_at, checked_at
    FROM found
    ON CONFLICT (repo_id, path) DO UPDATE SET
      blob_oid   = EXCLUDED.blob_oid,
      file_type  = EXCLUDED.file_type,
      directory  = EXCLUDED.directory,
      checked_at = EXCLUDED.checked_at,
      deleted_at = NULL,
      change_detected_at = CASE
        WHEN repo_well_known_files.blob_oid <> EXCLUDED.blob_oid
          OR repo_well_known_files.deleted_at IS NOT NULL
        THEN EXCLUDED.checked_at
        ELSE repo_well_known_files.change_detected_at
      END
    `,
    [JSON.stringify(batch)],
  )
}
