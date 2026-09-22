import { GenerateStreamsHandler } from '../../types'
import { DiscordRootStreamData, DiscordStreamType, IDiscordIntegrationSettings } from './types'

const handler: GenerateStreamsHandler = async (ctx) => {
  const settings = ctx.integration.settings as IDiscordIntegrationSettings

  const settingsChannels = settings.channels || []

  if (settings.forumChannels) {
    for (const forumChannel of settings.forumChannels) {
      settingsChannels.push({
        id: forumChannel.id,
        name: forumChannel.name,
      })
    }
  }

  const guildId = ctx.integration.identifier

  await ctx.publishStream<DiscordRootStreamData>(DiscordStreamType.ROOT, {
    guildId,
    channels: settingsChannels,
  })
}

export default handler
