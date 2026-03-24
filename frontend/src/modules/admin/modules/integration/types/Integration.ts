export interface Integration<Settings> {
  id: string
  status: string
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
