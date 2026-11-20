import { assertCredentialKind } from '../../types'
import type { Channel, Credential } from '../../types'
import { fetchActivatedEvents } from './consoleEvents'
import { createSessionizeSessionHttp } from './sessionHttp'

export async function discoverSessionize(credential: Credential): Promise<Channel[]> {
  assertCredentialKind(credential, 'sessionize-organizer-account')
  const events = await fetchActivatedEvents(createSessionizeSessionHttp(credential))
  return events.map((event) => ({ channelId: event.eventKey, channelName: event.eventName }))
}
