import {
  upsertRepoDockerDailySnapshot,
  upsertRepoDockerRow,
} from '@crowd/data-access-layer/src/packages'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'

import { DockerhubRepoResult } from './types'

export async function upsertRepoDocker(
  qx: QueryExecutor,
  repoId: string | null,
  r: DockerhubRepoResult,
): Promise<void> {
  // Transactional so a missing partition on repo_docker_pulls_daily (or any other
  // failure on the snapshot insert) rolls back the last_synced_at bump on repo_docker
  // too — otherwise the row drops out of the refresh queue for 24h with no snapshot.
  await qx.tx(async (tx) => {
    await upsertRepoDockerRow(tx, {
      repoId,
      imageName: r.imageName,
      pulls: r.pulls,
      stars: r.stars,
    })
    await upsertRepoDockerDailySnapshot(tx, r.imageName, r.pulls)
  })
}
