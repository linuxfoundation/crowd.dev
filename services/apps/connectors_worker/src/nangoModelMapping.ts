import { getGithubSyncForConnectorSync } from '@crowd/nango'

export function getNangoModelForSync(syncName: string): string | null {
  return getGithubSyncForConnectorSync(syncName)?.model ?? null
}
