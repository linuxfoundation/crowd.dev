import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { DockerhubRepoResult } from './types'

export async function upsertRepoDocker(
  qx: QueryExecutor,
  repoId: string | null,
  r: DockerhubRepoResult,
): Promise<void> {
  await qx.result(
    `
    INSERT INTO repo_docker (repo_id, image_name, pulls, stars, last_synced_at)
    VALUES ($(repoId), $(imageName), $(pulls), $(stars), NOW())
    ON CONFLICT (image_name) DO UPDATE SET
      repo_id        = COALESCE(repo_docker.repo_id, EXCLUDED.repo_id),
      pulls          = EXCLUDED.pulls,
      stars          = EXCLUDED.stars,
      last_synced_at = NOW()
    `,
    { repoId, imageName: r.imageName, pulls: r.pulls, stars: r.stars },
  )

  await qx.result(
    `
    INSERT INTO repo_docker_pulls_daily (image_name, date, pulls_total)
    VALUES ($(imageName), CURRENT_DATE, $(pulls))
    ON CONFLICT (image_name, date) DO UPDATE SET pulls_total = EXCLUDED.pulls_total
    `,
    { imageName: r.imageName, pulls: r.pulls },
  )
}

export async function touchRepoDocker(qx: QueryExecutor, imageName: string): Promise<void> {
  await qx.result(`UPDATE repo_docker SET last_synced_at = NOW() WHERE image_name = $(imageName)`, {
    imageName,
  })
}

export async function markDockerChecked(qx: QueryExecutor, repoId: string): Promise<void> {
  await qx.result(`UPDATE repos SET docker_checked_at = NOW() WHERE id = $(repoId)`, { repoId })
}
