export interface Integration<Settings> {
  id: string
  platform: string
  status: string
  isNango?: boolean
  settings: Settings
}

export interface IntegrationMapping {
  gitIntegrationId?: string
  sourceIntegrationId?: string
  url: string
  sourcePlatform: string
  segment: {
    id: string
    name: string
  }
}
