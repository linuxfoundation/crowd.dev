import { assertCredentialKind } from '../../types'
import type { Channel, Credential } from '../../types'

// The 500 req/hr budget is shared org-wide, so there is one global channel, owned (with the
// single mentionPoll unit) only by the segment-less anchor; subproject integrations own none.
export const OCTOLENS_GLOBAL_CHANNEL_ID = 'octolens-global'

export async function discoverOctolens(credential: Credential): Promise<Channel[]> {
  assertCredentialKind(credential, 'octolens-token')

  return [{ channelId: OCTOLENS_GLOBAL_CHANNEL_ID, channelName: OCTOLENS_GLOBAL_CHANNEL_ID }]
}
