export enum ChannelType {
  /**
   * A text channel within a guild
   */
  GuildText = 0,
  /**
   * A direct message between users
   */
  DM = 1,
  /**
   * A voice channel within a guild
   */
  GuildVoice = 2,
  /**
   * A direct message between multiple users
   */
  GroupDM = 3,
  /**
   * An organizational category that contains up to 50 channels
   *
   * See https://support.discord.com/hc/articles/115001580171
   */
  GuildCategory = 4,
  /**
   * A channel that users can follow and crosspost into their own guild
   *
   * See https://support.discord.com/hc/articles/360032008192
   */
  GuildAnnouncement = 5,
  /**
   * A temporary sub-channel within a Guild Announcement channel
   */
  AnnouncementThread = 10,
  /**
   * A temporary sub-channel within a Guild Text or Guild Forum channel
   */
  PublicThread = 11,
  /**
   * A temporary sub-channel within a Guild Text channel that is only viewable by those invited and those with the Manage Threads permission
   */
  PrivateThread = 12,
  /**
   * A voice channel for hosting events with an audience
   *
   * See https://support.discord.com/hc/articles/1500005513722
   */
  GuildStageVoice = 13,
  /**
   * The channel in a Student Hub containing the listed servers
   *
   * See https://support.discord.com/hc/articles/4406046651927
   */
  GuildDirectory = 14,
  /**
   * A channel that can only contain threads
   */
  GuildForum = 15,
}

/**
 * https://discord.com/developers/docs/resources/channel#message-object-message-types
 */
export enum MessageType {
  Default = 0,
  RecipientAdd = 1,
  RecipientRemove = 2,
  Call = 3,
  ChannelNameChange = 4,
  ChannelIconChange = 5,
  ChannelPinnedMessage = 6,
  UserJoin = 7,
  GuildBoost = 8,
  GuildBoostTier1 = 9,
  GuildBoostTier2 = 10,
  GuildBoostTier3 = 11,
  ChannelFollowAdd = 12,
  GuildDiscoveryDisqualified = 14,
  GuildDiscoveryRequalified = 15,
  GuildDiscoveryGracePeriodInitialWarning = 16,
  GuildDiscoveryGracePeriodFinalWarning = 17,
  ThreadCreated = 18,
  Reply = 19,
  ChatInputCommand = 20,
  ThreadStarterMessage = 21,
  GuildInviteReminder = 22,
  ContextMenuCommand = 23,
  AutoModerationAction = 24,
  RoleSubscriptionPurchase = 25,
  InteractionPremiumUpsell = 26,
  StageStart = 27,
  StageEnd = 28,
  StageSpeaker = 29,
  /**
   * @unstable https://github.com/discord/discord-api-docs/pull/5927#discussion_r1107678548
   */
  StageRaiseHand = 30,
  StageTopic = 31,
  GuildApplicationPremiumSubscription = 32,
}
