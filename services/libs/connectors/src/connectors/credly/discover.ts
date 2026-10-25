import { assertCredentialKind } from '../../types'
import type { Channel, Credential } from '../../types'

// One organization, one channel: every badge the Linux Foundation issues comes through it,
// routing to subprojects happens at emit time, not through channels.
export async function discoverCredly(credential: Credential): Promise<Channel[]> {
  assertCredentialKind(credential, 'credly-oauth-client')
  const { organizationId } = credential.data
  return [{ channelId: organizationId, channelName: organizationId }]
}
