import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { RepoActivitySnapshot } from './types'

export async function bulkUpsertRepoActivitySnapshot(
  qx: QueryExecutor,
  rows: RepoActivitySnapshot[],
): Promise<void> {
  if (rows.length === 0) return

  await qx.result(
    `
    WITH upserted AS (
      INSERT INTO repo_activity_snapshot (
        repo_id,
        snapshot_at,
        window_months,
        commits_last_12m,
        commits_last_6m,
        commits_prior_6m,
        prs_opened_last_12m,
        prs_merged_last_12m,
        prs_closed_unmerged_12m,
        pr_median_time_to_merge_hours,
        pr_median_time_to_first_response_hours,
        issues_opened_last_12m,
        issues_closed_last_12m,
        issues_opened_last_6m,
        issues_opened_prior_6m,
        issues_open_now,
        issue_median_time_to_close_hours,
        issue_median_time_to_first_response_hours
      )
      SELECT
        (j->>'repoId')::bigint,
        (j->>'snapshotAt')::timestamptz,
        (j->>'windowMonths')::int,
        (j->>'commitsLast12m')::int,
        (j->>'commitsLast6m')::int,
        (j->>'commitsPrior6m')::int,
        (j->>'prsOpenedLast12m')::int,
        (j->>'prsMergedLast12m')::int,
        (j->>'prsClosedUnmerged12m')::int,
        (j->>'prMedianTimeToMergeHours')::int,
        (j->>'prMedianTimeToFirstResponseHours')::int,
        (j->>'issuesOpenedLast12m')::int,
        (j->>'issuesClosedLast12m')::int,
        (j->>'issuesOpenedLast6m')::int,
        (j->>'issuesOpenedPrior6m')::int,
        (j->>'issuesOpenNow')::int,
        (j->>'issueMedianTimeToCloseHours')::int,
        (j->>'issueMedianTimeToFirstResponseHours')::int
      FROM jsonb_array_elements($1::jsonb) j
      ON CONFLICT (repo_id) DO UPDATE SET
        snapshot_at                               = EXCLUDED.snapshot_at,
        window_months                             = EXCLUDED.window_months,
        commits_last_12m                          = EXCLUDED.commits_last_12m,
        commits_last_6m                           = EXCLUDED.commits_last_6m,
        commits_prior_6m                          = EXCLUDED.commits_prior_6m,
        prs_opened_last_12m                       = EXCLUDED.prs_opened_last_12m,
        prs_merged_last_12m                       = EXCLUDED.prs_merged_last_12m,
        prs_closed_unmerged_12m                   = EXCLUDED.prs_closed_unmerged_12m,
        pr_median_time_to_merge_hours             = EXCLUDED.pr_median_time_to_merge_hours,
        pr_median_time_to_first_response_hours    = EXCLUDED.pr_median_time_to_first_response_hours,
        issues_opened_last_12m                    = EXCLUDED.issues_opened_last_12m,
        issues_closed_last_12m                    = EXCLUDED.issues_closed_last_12m,
        issues_opened_last_6m                     = EXCLUDED.issues_opened_last_6m,
        issues_opened_prior_6m                    = EXCLUDED.issues_opened_prior_6m,
        issues_open_now                           = EXCLUDED.issues_open_now,
        issue_median_time_to_close_hours          = EXCLUDED.issue_median_time_to_close_hours,
        issue_median_time_to_first_response_hours = EXCLUDED.issue_median_time_to_first_response_hours
      RETURNING repo_id, snapshot_at
    )
    UPDATE repos r
    SET snapshot_at = u.snapshot_at
    FROM upserted u
    WHERE r.id = u.repo_id
    `,
    [JSON.stringify(rows)],
  )
}
