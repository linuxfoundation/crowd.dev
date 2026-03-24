export enum HackerNewsActivityType {
  POST = 'post',
  COMMENT = 'comment',
}

export enum HackerNewsStreamType {
  INITIAL = 'initial',
  MAIN = 'main',
}

export interface HackerNewsPublishData {
  post: HackerNewsResponse
  channel: string
  parentId?: string
  parentTitle?: string
}

export interface HackerNewsInitialStreamMetadata {
  keywords: string[]
}

export interface HackerNewsMainStreamMetadata {
  postId: number
  channel: string
  parentId?: string
  parentTitle?: string
}

export interface HackerNewsSearchResponseRaw {
  hits: [
    {
      created_at: string
      title: string
      url: string
      author: string
      points: number
      story_text: string
      comment_text: string | null
      num_comments: number
      story_id: string | null
      story_title: string | null
      story_url: string | null
      parent_id: string | null
      created_at_i: number
      _tags: string[]
      objectID: string
      _highlightResult: {
        [key: string]: {
          value: string
          matchLevel: string
          matchedWords: string[]
          fullyHighlighted?: boolean
        }
      }
    },
  ]
}

export interface HackerNewsSearchResult {
  keywords: string[]
  postId: number
}

export interface HackerNewsPost {
  by: string
  descendants: number
  id: number
  kids?: number[]
  parent?: number
  score: number
  time: number
  title: string
  text: string
  type: string
  url: string
}

export interface HackerNewsUser {
  about: string
  created: number
  id: string
  karma: number
  submitted: number[]
}

export interface HackerNewsResponse extends HackerNewsPost {
  user: HackerNewsUser
}

export { IHackernewsIntegrationSettings as HackerNewsIntegrationSettings } from '@crowd/types'

export interface HackerNewsKeywordSearchInput {
  keywords: string[]
  after: number
}
