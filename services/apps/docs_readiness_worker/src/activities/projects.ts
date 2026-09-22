import {
  IFindProjectsForDocsReadiness,
  IProjectForDocsReadiness,
  findProjectsForDocsReadiness,
} from '@crowd/data-access-layer'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'

import { svc } from '../main'

export async function findProjectsForSweep(
  args: IFindProjectsForDocsReadiness,
): Promise<IProjectForDocsReadiness[]> {
  const qx = pgpQx(svc.postgres.reader.connection())
  return findProjectsForDocsReadiness(qx, args)
}
