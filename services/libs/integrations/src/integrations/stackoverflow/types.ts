export const STACKOVERFLOW_MAX_RETROSPECT_IN_HOURS = 7
export const STACKOVERFLOW_LAST_MAX_PAGES = 20

export enum StackOverflowActivityType {
  QUESTION = 'question',
  ANSWER = 'answer',
}

export interface StackOverflowPlatformSettings {
  key: string
}

export { IStackoverflowIntegrationSettings as IStackOverflowIntegrationSettings } from '@crowd/types'

export interface IStackOverflowPublishQuestion {
  question: StackOverflowShallowQuestion
  user: StackOverflowUser
  tag: string | null
  keyword: string | null
}

export interface IStackOverflowPublishData {
  question: IStackOverflowPublishQuestion | null
  answer: IStackOverflowPublishAnswer | null
}

export interface IStackOverflowPublishAnswer {
  answer: StackOverflowAnswer
  user: StackOverflowUser
  tag: string | null
  keyword: string | null
  previousAnswerId: string | null
}

export interface IStackOverflowTagStreamData {
  tags: string[]
  page: number
}

export interface IStackOverflowAnswerStreamData {
  questionId: string
  page: number
  tag: string | null
  keyword: string | null
}

export interface IStackOverflowKeywordStreamData {
  keyword: string
  page: number
}

export enum StackOverflowRootStream {
  QUESTIONS_BY_TAG = 'questions_by_tag',
  QUESTIONS_BY_KEYWORD = 'questions_by_keyword',
  ANSWERS_TO_QUESTION = 'answers_to_question',
}

interface StackOverflowBase {
  backoff?: number
  error_id?: number
  error_message?: string
  error_name?: string
  has_more: boolean
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  items: any[]
  page?: number
  page_size?: number
  quota_max: number
  quota_remaining: number
  total?: number
  type?: string
}

export interface StackOverflowGetQuestionsInput {
  tags: string[]
  nangoId: string
  page: number // 1-based
}

export interface StackOverflowGetQuestionsByKeywordInput {
  keyword: string
  nangoId: string
  page: number // 1-based
}

export interface StackOverflowAnswersInput {
  questionId: string
  nangoId: string
  page: number // 1-based
}

export interface StackOverflowUserInput {
  userId: string
  nangoId: string
}

export interface StackOverflowAnswerResponse extends StackOverflowBase {
  items: StackOverflowAnswer[]
}

export interface StackOverflowTagsResponse extends StackOverflowBase {
  items:
    | [
        {
          has_synonyms: boolean
          is_moderator_only: boolean
          is_required: boolean
          count: number
          name: string
        },
      ]
    | []
}

export interface StackOverflowQuestionsResponse extends StackOverflowBase {
  items: StackOverflowShallowQuestion[]
}

export interface StackOverflowUserResponse extends StackOverflowBase {
  items: StackOverflowUser[]
}

export interface StackOverflowUser {
  about_me?: string
  accept_rate?: number
  account_id: number
  age?: number
  answer_count?: number
  badge_counts?: StackOverflowBadgeCount
  collectives?: StackOverflowCollective[]
  creation_date: number
  display_name: string
  down_vote_count?: number
  is_employee: boolean
  last_access_date: number
  last_modified_date?: number
  link: string
  location?: string
  profile_image: string
  question_count?: number
  reputation: number
  reputation_change_day: number
  reputation_change_month: number
  reputation_change_quarter: number
  reputation_change_week: number
  reputation_change_year: number
  timed_penalty_date?: number
  up_vote_count?: number
  user_id: string
  user_type: 'unregistered' | 'registered' | 'moderator' | 'does_not_exist' | 'team_admin'
  website_url?: string
}

export interface StackOverflowShallowQuestion {
  tags: string[]
  owner: StackOverflowShallowUser
  is_answered: boolean
  view_count: number
  answer_count: number
  score: number
  last_activity_date: number
  creation_date: number
  question_id: number
  content_license: string
  link: string
  title: string
  body: string
}

export interface StackOverflowSearchResponse {
  items: StackOverflowQuestion[]
}

export interface StackOverflowQuestion {
  accepted_answer_id?: number
  answer_count: number
  answers?: StackOverflowAnswer[]
  body: string
  body_markdown: string
  bounty_amount?: number
  bounty_closes_date?: number
  bounty_user?: StackOverflowShallowUser
  can_answer: boolean
  can_close: boolean
  can_comment: boolean
  can_edit: boolean
  can_flag: boolean
  can_suggest_edit: boolean
  close_vote_count: number
  closed_date?: number
  closed_details?: StackOverflowClosedDetails
  closed_reason: string
  collectives: StackOverflowCollective[]
  comment_count: number
  comments?: StackOverflowComment[]
  community_owned_date?: number
  content_license: string
  creation_date: number
  delete_vote_count: number
  down_vote_count: number
  downvoted: boolean
  favorite_count: number
  favorited: boolean
  is_answered: boolean
  last_activity_date: number
  last_edit_date?: number
  last_editor?: StackOverflowShallowUser
  link: string
  locked_date?: number
  migrated_from?: StackOverflowMigrationInfo
  migrated_to?: StackOverflowMigrationInfo
  notice?: StackOverflowNotice
  owner: StackOverflowShallowUser
  posted_by_collectives: StackOverflowCollective[]
  protected_date?: number
  question_id: number
  reopen_vote_count: number
  score: number
  share_link: string
  tags: string[]
  title: string
  up_vote_count: number
  upvoted: boolean
  view_count: number
}

export interface StackOverflowNotice {
  body: string
  creation_date: number
  owner_user_id: number
}

export interface StackOverflowMigrationInfo {
  on_date: number
  other_site: StackOverflowSite
  question_id: number
}

export interface StackOverflowSite {
  aliases: string[]
  api_site_parameter: string
  audience: string
  closed_beta_date?: number
  favicon_url: string
  high_resolution_icon_url: string
  icon_url: string
  launch_date: number
  logo_url: string
  markdown_extensions?: string[]
  name: string
  open_beta_date?: number
  related_sites?: StackOverflowRelatedSite[]
  site_state: 'normal' | 'closed_beta' | 'open_beta' | 'linked_meta'
  site_type: 'main_site' | 'meta_site'
  site_url: string
  styling: StackOverflowStyling
  twitter_account?: string
}

export interface StackOverflowStyling {
  link_color: string
  tag_background_color: string
  tag_foreground_color: string
}

export interface StackOverflowRelatedSite {
  api_site_parameter: string
  name: string
  relation: 'parent' | 'meta' | 'chat'
  site_url: string
}

export interface StackOverflowClosedDetails {
  by_users?: StackOverflowShallowUser[]
  description: string
  on_hold: boolean
  original_questions?: StackOverflowOriginalQuestion[]
  reason: string
}

export interface StackOverflowOriginalQuestion {
  accepted_answer_id?: number
  answer_count: number
  question_id: number
  title: string
}

export interface StackOverflowAnswer {
  accepted: boolean
  answer_id: number
  awarded_bounty_amount?: number
  awarded_bounty_users?: StackOverflowShallowUser[]
  body: string
  body_markdown: string
  can_comment: boolean
  can_edit: boolean
  can_flag: boolean
  can_suggest_edit: boolean
  collectives: StackOverflowCollective[]
  comment_count: number
  comments?: StackOverflowComment[]
  community_owned_date?: number
  content_license: string
  creation_date: number
  down_vote_count: number
  downvoted: boolean
  is_accepted: boolean
  last_activity_date: number
  last_edit_date?: number
  last_editor?: StackOverflowShallowUser
  link: string
  locked_date?: number
  owner?: StackOverflowShallowUser
  posted_by_collectives: StackOverflowCollective[]
  question_id: number
  recommendations?: StackOverflowCollectiveRecommendation[]
  score: number
  share_link: string
  tags: string[]
  title: string
  up_vote_count: number
  upvoted: boolean
}

export interface StackOverflowCollectiveRecommendation {
  collective: StackOverflowCollective
  creation_date: number
}

export interface StackOverflowCollectiveMembership {
  collective: StackOverflowCollective
  role: 'admin' | 'recognized_member' | 'member'
}

export interface StackOverflowComment {
  body: string
  body_markdown: string
  can_flag: boolean
  comment_id: number
  content_license: string
  creation_date: number
  edited: boolean
  link: string
  owner: StackOverflowShallowUser
  post_id: number
  post_type: 'question' | 'answer' | 'article'
  reply_to_user: StackOverflowShallowUser
  score: number
  upvoted: boolean
}

export interface StackOverflowCollective {
  description: string
  external_links: StackOverflowCollectiveExternalLink[]
  link: string
  name: string
  slug: string
  tags: string[]
}

export interface StackOverflowCollectiveExternalLink {
  link: string
  type: 'website' | 'twitter' | 'github' | 'facebook' | 'instagram' | 'support' | 'linkedin'
}

export interface StackOverflowShallowUser {
  accept_rate?: number
  account_id: number
  badge_counts?: StackOverflowBadgeCount
  display_name?: string
  link?: string
  profile_image?: string
  reputation?: number
  user_id?: string
  user_type: 'unregistered' | 'registered' | 'moderator' | 'team_admin' | 'does_not_exist'
}

export interface StackOverflowBadgeCount {
  bronze: number
  gold: number
  silver: number
}
