// generateStreams.ts content
import { GenerateStreamsHandler } from '../../types'
import {
  GroupsioGroupMembersStreamMetadata,
  GroupsioIntegrationSettings,
  GroupsioPastGroupMembersStreamMetadata,
  GroupsioStreamType,
} from './types'

const handler: GenerateStreamsHandler = async (ctx) => {
  const settings = ctx.integration.settings as GroupsioIntegrationSettings
  const groups = settings.groups
  const token = settings.token
  const email = settings.email

  const onboarding = ctx.onboarding

  if (!groups || groups.length === 0) {
    await ctx.abortRunWithError('No groups specified!')
  }

  if (!token) {
    await ctx.abortRunWithError('No token specified!')
  }

  if (!email) {
    await ctx.abortRunWithError('No email specified!')
  }

  for (const group of groups) {
    // here we start parsing group members - very important to do this first
    // because we need to know who the members are before we can start parsing
    // messages don't have enough information to create members
    await ctx.publishStream<GroupsioGroupMembersStreamMetadata>(
      `${GroupsioStreamType.GROUP_MEMBERS}:${group.slug}`,
      {
        group: group.slug,
        page: null,
      },
    )
    // also parse past group members but only when onboarding is true or group was added recently
    if (
      onboarding ||
      (group?.groupAddedOn &&
        (new Date().getTime() - new Date(group?.groupAddedOn).getTime()) / (1000 * 60 * 60 * 24) <=
          5)
    ) {
      await ctx.publishStream<GroupsioPastGroupMembersStreamMetadata>(
        `${GroupsioStreamType.PAST_GROUP_MEMBERS}:${group.slug}`,
        {
          group: group.slug,
          page: null,
        },
      )
    }
  }
}

export default handler
