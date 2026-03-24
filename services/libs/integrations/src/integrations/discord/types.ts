export enum DiscordActivityType {
  JOINED_GUILD = 'joined_guild',
  MESSAGE = 'message',
  THREAD_STARTED = 'thread_started',
  THREAD_MESSAGE = 'thread_message',
}

export { IDiscordIntegrationSettings } from '@crowd/types'

export interface IDiscordPlatformSettings {
  token: string
  token2: string
}

export enum DiscordStreamType {
  ROOT = 'root',
  CHANNEL = 'channel',
  MEMBERS = 'members',
  THREADS = 'threads',
}

export enum DiscordAPIDataType {
  CHANNEL = 'channel',
  MEMBER = 'member',
}

export interface IDiscordAPIData {
  type: DiscordAPIDataType
  data: DiscordApiMember[] | DiscordApiDataMessage
}

export interface DiscordRootStreamData {
  guildId: string
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  channels: any[]
}

export interface DiscordMemberStreamData {
  page: string
  guildId: string
  channels: DiscordApiChannel[]
}

export interface DiscordChannelStreamData {
  channelId: string
  guildId: string
  page: string
  channels: DiscordApiChannel[]
}

export interface DiscordGetChannelsInput {
  guildId: string
  token: string
}

export interface DiscordGetMessagesInput {
  channelId: string
  token: string
  page: string | undefined
  perPage: number | 100
  showError?: boolean
}

export interface DiscordGetMembersInput {
  guildId: string
  token: string
  page: string | undefined
  perPage: number | 100
}

export interface DiscordParsedReponse {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  records: any
  nextPage: string
  limit: number
  timeUntilReset: number
}

export interface DiscordGetMessagesOutput extends DiscordParsedReponse {
  records: DiscordApiMessage[]
}

export interface DiscordGetMembersOutput extends DiscordParsedReponse {
  records: DiscordApiMember[]
}

export interface DiscordApiChannel {
  id: string
  type: number
  guild_id?: string
  name?: string
  topic?: string
  nsfw?: boolean
  last_message_id?: string
  parent_id?: string
  flags?: number
}

export interface DiscordApiChannelMention {
  id: string
  guild_id: string
  type: number
  name: string
}

export interface DiscordApiMessageReference {
  message_id?: string
  channel_id?: string
  guild_id?: string
  fail_if_not_exists?: boolean
}

export interface DiscordApiReaction {
  count: number
  me: boolean
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  emoji: any
}

export interface DiscordApiAttachment {
  id: string
  filename: string
  description?: string
  content_type?: string
  size: number
  url: string
  proxy_url: string
  height?: number
  width?: number
  ephemeral?: boolean
}

export interface DiscordApiMessage {
  id: string
  channel_id: string
  author?: DiscordApiUser
  content: string
  timestamp: string
  edited_timestamp: string | null
  mention_everyone: boolean
  mentions: DiscordApiUser[]
  mention_roles: string[]
  mention_channels: DiscordApiChannelMention[]
  pinned: boolean
  type: number
  message_reference?: DiscordApiMessageReference
  flags?: number
  referenced_message?: DiscordApiMessage
  thread?: DiscordApiChannel
  reactions?: DiscordApiReaction[]
  attachments?: DiscordApiAttachment[]
}

export interface DiscordApiUser {
  id: string
  username: string
  discriminator: string
  avatar?: string
  bot?: boolean
  system?: boolean
  banner?: string
  accent_color?: number
  locale?: string
  verified?: boolean
  email?: string
  flags?: number
  premium_type?: number
  public_flags?: number
}

export interface DiscordApiMember {
  user?: DiscordApiUser
  nick?: string
  avatar?: string
  roles: string[]
  joined_at: string
  premium_since?: string
  deaf: boolean
  mute: boolean
  flags: number
  pending?: boolean
  permissions?: string
  communication_disabled_until?: string
}

export interface DiscordApiDataMessage extends DiscordApiMessage {
  parent?: string
  parentChannel?: string
  isForum?: boolean
  isThread?: boolean
  channel?: DiscordApiChannel
}
