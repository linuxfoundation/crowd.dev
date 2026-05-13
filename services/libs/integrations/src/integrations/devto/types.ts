import { IDevToArticle } from './api/articles'
import { IDevToComment } from './api/comments'

export {
  IDevtoArticleSettings as IDevToArticleSettings,
  IDevtoIntegrationSettings as IDevToIntegrationSettings,
} from '@crowd/types'

export enum DevToRootStream {
  ORGANIZATION_ARTICLES = 'organization_articles',
  USER_ARTICLES = 'user_articles',
}

export enum DevToActivityType {
  COMMENT = 'comment',
}

export interface IDevToRootOrganizationStreamData {
  organization: string
}

export interface IDevToRootUserStreamData {
  user: string
}

export interface IDevToArticleData {
  article: IDevToArticle
  comments: IDevToComment[]
}

export interface DevToPlatformSettings {
  apiKey: string
}
