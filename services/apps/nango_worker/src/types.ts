export interface IProcessNangoWebhookArguments {
  connectionId: string
  providerConfigKey: string
  model: string
  syncType: 'INITIAL' | 'INCREMENTAL'
  nextPageCursor?: string
}
