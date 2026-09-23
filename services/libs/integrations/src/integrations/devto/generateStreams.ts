import { GenerateStreamsHandler } from '../../types'
import {
  DevToRootStream,
  IDevToIntegrationSettings,
  IDevToRootOrganizationStreamData,
  IDevToRootUserStreamData,
} from './types'

const handler: GenerateStreamsHandler = async (ctx) => {
  const settings = ctx.integration.settings as IDevToIntegrationSettings

  if (settings.organizations.length === 0 && settings.users.length === 0) {
    await ctx.abortRunWithError('No organizations or users configured!')
    return
  }

  if (settings.organizations.length > 0) {
    for (const organization of settings.organizations) {
      await ctx.publishStream<IDevToRootOrganizationStreamData>(
        `${DevToRootStream.ORGANIZATION_ARTICLES}-${organization}`,
        {
          organization,
        },
      )
    }
  }
  if (settings.users.length > 0) {
    for (const user of settings.users) {
      await ctx.publishStream<IDevToRootUserStreamData>(
        `${DevToRootStream.USER_ARTICLES}-${user}`,
        {
          user,
        },
      )
    }
  }
}

export default handler
