import { hasEmittingSyncUnit } from '@crowd/data-access-layer/src/connectors'
import { getNangoMappingByConnectionId } from '@crowd/data-access-layer/src/integrations'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getGithubSyncForNangoModel, githubChannelName } from '@crowd/nango'

export async function isSwitchedToConnectors(
  qx: QueryExecutor,
  integrationId: string,
  connectionId: string,
  model: string,
): Promise<boolean> {
  const sync = getGithubSyncForNangoModel(model)
  if (!sync) {
    return false
  }

  const mapping = await getNangoMappingByConnectionId(qx, connectionId)
  if (!mapping) {
    return false
  }

  return hasEmittingSyncUnit(
    qx,
    integrationId,
    githubChannelName(mapping.owner, mapping.repoName),
    sync.connectorSync,
  )
}
