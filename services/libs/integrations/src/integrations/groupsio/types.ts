// types.ts content
export enum GroupsioActivityType {
  MEMBER_JOIN = 'member_join',
  MESSAGE = 'message',
  MEMBER_LEAVE = 'member_leave',
}

export type GroupName = string

export enum GroupsioStreamType {
  GROUP = 'group',
  GROUP_MEMBERS = 'group_members',
  PAST_GROUP_MEMBERS = 'past_group_members',
  TOPIC = 'topic',
  ACTIVITY_LOGS = 'activity_logs',
}

export enum GroupsioPublishDataType {
  MESSAGE = 'message',
  MEMBER_JOIN = 'member_join',
  MEMBER_LEFT = 'member_left',
}

export interface GroupsioMessageData {
  group: GroupName
  topic: Topic
  message: Message
  member: MemberInfo | MemberInfoMinimal
  sourceParentId: string | null
}

export interface GroupsioMemberJoinData {
  group: GroupName
  member: MemberInfo
  joinedAt: string
}

export interface GroupsioMemberLeftData {
  group: GroupName
  member: MemberInfo
  leftAt: string
}

export interface GroupsioGroupStreamMetadata {
  group: GroupName
  page: string | null
  groupsioPageNumber: number
}

export interface GroupsioGroupMembersStreamMetadata {
  group: GroupName
  page: string | null
}

export interface GroupsioPastGroupMembersStreamMetadata {
  group: GroupName
  page: string | null
}

export interface GroupsioMemberStreamMetadata {
  group: GroupName
  memberId: string
}

export interface GroupsioTopicStreamMetadata {
  group: GroupName
  topic: Topic
  page: string | null
}

export interface GroupsioActivityLogsStreamMetadata {
  group: GroupName
  page: string | null
}

export interface GroupsioPublishData<T> {
  type: GroupsioPublishDataType
  data: T
}

export {
  IGroupsioIntegrationSettings as GroupsioIntegrationSettings,
  IGroupsioGroupDetails as GroupDetails,
} from '@crowd/types'

export interface ActivityLog {
  id: number
  object: 'activity_log'
  created: string
  group_id: number
  entry: string
  via: string
}

export interface Topic {
  id: number
  object: 'topic'
  created: string
  updated: string
  group_id: number
  group_subject_tag: string
  subject: string
  summary: string
  name: string
  profile_photo_url: string
  num_messages: number
  is_sticky: boolean
  is_moderated: boolean
  is_closed: boolean
  has_attachments: boolean
  reply_to: string
  most_recent_message: string
  hashtags: null | string[]
}

export interface ListActivityLogs {
  object: 'list'
  total_count: number
  start_item: number
  end_item: number
  has_more: boolean
  next_page_token: number
  sort_field: string
  second_order: string
  query: string
  sort_dir: 'asc' | 'desc'
  data: ActivityLog[]
}

export interface ListBase {
  object: 'list'
  total_count: number
  start_item: number
  end_item: number
  has_more: boolean
  next_page_token: number
  sort_field: string
  second_order: string
  query: string
  sort_dir: 'asc' | 'desc'
}

export interface ListTopics extends ListBase {
  data: Topic[]
}

export interface ListMessages extends ListBase {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  group_perms?: any
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  group?: any
  cover_photo_url?: string
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  sub_data?: any
  topic: Topic
  data: Message[]
}

export interface Message {
  id: number
  object: string
  created: string
  updated: string
  user_id: number
  group_id: number
  topic_id: number
  body: string
  quoted: string
  remainder: string
  snippet: string
  subject: string
  subject_with_tags: string
  name: string
  profile_photo_url: string
  is_plain_text: boolean
  msg_num: number
  is_reply: boolean
  has_liked: boolean
  num_likes: number
  is_closed: boolean
  is_moderated: boolean
  reply_to: string
  can_repost: boolean
  hashtags: Hashtag[]
  poll_id: number
  attachments: Attachment[]
}

export interface Hashtag {
  id: number
  object: string
  created: string
  group_id: number
  name: string
  mods_only_post: boolean
  mods_only_replies: boolean
  no_email: boolean
  moderated: boolean
  special: boolean
  replies_unmoderated: boolean
  locked: boolean
  until: string
  close_instead_of_delete: boolean
  description: string
  color_name: string
  color_hex: string
  reply_to: string
  topic_count: number
  last_message_date: string
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  muted: null | any
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  followed: null | any
}

export interface Attachment {
  id: number
  media_type: string
  download_url: string
  image_thumbnail_url: string
  filename: string
}

interface Perms {
  object: string
  archives_visible: boolean
  polls_visible: boolean
  members_visible: boolean
  chat_visible: boolean
  calendar_visible: boolean
  files_visible: boolean
  database_visible: boolean
  photos_visible: boolean
  wiki_visible: boolean
  member_directory_visible: boolean
  hashtags_visible: boolean
  guidelines_visible: boolean
  subgroups_visible: boolean
  open_donations_visible: boolean
  sponsor_visible: boolean
  manage_subgroups: boolean
  delete_group: boolean
  download_archives: boolean
  download_entire_group: boolean
  download_members: boolean
  view_activity: boolean
  create_hashtags: boolean
  manage_hashtags: boolean
  manage_integrations: boolean
  manage_group_settings: boolean
  make_moderator: boolean
  manage_member_subscription_options: boolean
  manage_pending_members: boolean
  remove_members: boolean
  ban_members: boolean
  manage_group_billing: boolean
  manage_group_payments: boolean
  edit_archives: boolean
  manage_pending_messages: boolean
  invite_members: boolean
  can_post: boolean
  can_vote: boolean
  manage_polls: boolean
  manage_photos: boolean
  manage_members: boolean
  manage_calendar: boolean
  manage_chats: boolean
  view_member_directory: boolean
  manage_files: boolean
  manage_wiki: boolean
  manage_subscription: boolean
  public_page: boolean
  sub_page: boolean
  mod_page: boolean
}

interface ExtraMemberData {
  col_id: number
  col_type: string
  text?: string
  checked?: boolean
  date?: string
  time?: string
  street_address1?: string
  street_address2?: string
  city?: string
  state?: string
  zip?: string
  country?: string
  title?: string
  url?: string
  desc?: string
  image_name?: string
}

export interface MemberInfo {
  id: number
  object: string
  created: string
  updated: string
  user_id: number
  group_id: number
  group_name: string
  status: string
  post_status: string
  email_delivery: string
  message_selection: string
  auto_follow_replies: boolean
  max_attachment_size: string
  approved_posts: number
  mod_status: string
  pending_msg_notify: string
  pending_sub_notify: string
  sub_notify: string
  storage_notify: string
  sub_group_notify: string
  message_report_notify: string
  account_notify: string
  mod_permissions: string
  owner_msg_notify: string
  chat_notify: string
  photo_notify: string
  file_notify: string
  wiki_notify: string
  database_notify: string
  email: string
  user_status: string
  user_name: string
  timezone: string
  full_name: string
  about_me: string
  location: string
  website: string
  profile_privacy: string
  dont_munge_message_id: boolean
  use_signature: boolean
  use_signature_email: boolean
  signature: string
  color: string
  cover_photo_url: string
  icon_url: string
  nice_group_name: string
  subs_count: number
  most_recent_message: string
  perms: Perms
  extra_member_data: ExtraMemberData[]
}

export interface PastMemberInfo {
  id: number
  object: string
  joined: string
  created: string
  action: string
  member_info: MemberInfo
}

export interface MemberInfoMinimal {
  user_id: number
  full_name: string
  email: string
  group_id: number
}

export interface ListMembers {
  object: string
  total_count: number
  start_item: number
  end_item: number
  has_more: boolean
  next_page_token: number
  sort_field: string
  second_order: string
  query: string
  sort_dir: string
  data: MemberInfo[]
}

export interface ListPastMembers {
  object: string
  total_count: number
  start_item: number
  end_item: number
  has_more: boolean
  next_page_token: number
  sort_field: string
  second_order: string
  query: string
  sort_dir: string
  data: PastMemberInfo[]
}

export enum GroupsioWebhookEventType {
  JOINED = 'joined',
  SENT_MESSAGE_ACCEPTED = 'sent_message_accepted',
  LEFT = 'left',
}

export interface GroupsioWebhookPayload<T> {
  event: GroupsioWebhookEventType
  data: T
  signature: string
}

export interface Group {
  id: number
  object: string
  created: string
  updated: string
  type: string
  title: string
  name: string
  nice_group_name: string
  alias: string
  desc: string
  plain_desc: string
}

export interface GroupsioWebhookJoinPayload {
  id: number
  object: string
  created: string
  webhook_id: number
  action: string
  via: string
  group: Group
  member_info: MemberInfo
}
