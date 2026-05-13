import { QueryExecutor } from '../queryExecutor'

export enum MaintainerRepoType {
  GITHUB = 'github',
  GITLAB = 'gitlab',
  GIT = 'git',
  GERRIT = 'gerrit',
}

export interface Maintainer {
  id: string
  memberId: string
  segmentId: string
  dateStart: string
  dateEnd?: string
  url: string
  repoType: MaintainerRepoType
  role: string
  maintainerFile: string | null
}

export async function findMaintainerRoles(
  qx: QueryExecutor,
  memberIds: string[],
): Promise<Maintainer[]> {
  return qx.select(
    `
      SELECT
        mmr.*,
        rp."maintainerFile"
      FROM mv_maintainer_roles mmr
      LEFT JOIN public.repositories r ON r.url = mmr.url
      LEFT JOIN git."repositoryProcessing" rp ON rp."repositoryId" = r.id
      WHERE mmr."memberId" IN ($(memberIds:csv))
    `,
    {
      memberIds,
    },
  )
}
