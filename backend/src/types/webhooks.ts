import { BaseError } from './baseError'

export enum WebhookState {
  PENDING = 'PENDING',
  PROCESSED = 'PROCESSED',
  PROCESSING = 'PROCESSING',
  ERROR = 'ERROR',
}

export enum WebhookType {
  GITHUB = 'GITHUB',
  DISCOURSE = 'DISCOURSE',
  GROUPSIO = 'GROUPSIO',
}

export interface GithubWebhookPayload {
  signature: string
  event: string
  data: any
}

export type IncomingWebhookPayload = GithubWebhookPayload

export interface IncomingWebhookData {
  id: string
  tenantId: string
  integrationId: string
  state: WebhookState
  type: WebhookType
  payload: IncomingWebhookPayload
  processedAt: string | null
  error: any | null
  createdAt: string
}

export interface DbIncomingWebhookInsertData {
  tenantId: string
  integrationId: string
  type: WebhookType
  payload: any
}

export interface PendingWebhook {
  id: string
  tenantId: string
}

export interface ErrorWebhook extends PendingWebhook {}

export class WebhookError extends BaseError {
  public webhookId: string

  constructor(webhookId: string, message: string, origError?: any) {
    super(message, origError)
    this.webhookId = webhookId
  }
}
