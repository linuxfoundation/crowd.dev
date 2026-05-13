// import { IntegrationConfig } from '@/config/integrations'
// import { IntegrationStatusConfig } from '../../integration/config/status'
export interface OverviewTrends {
  current: number
  previous: number
  period: string
}

export interface IntegrationTabs {
  label: string
  key: string
  count: number
  icon: string
}

// TODO: Check with backend team about the data structure
export interface IntegrationStatus {
  grandparentId: string
  grandparentName: string
  id: string
  name: string
  parentId: string
  parentName: string
  platform: string
  segmentId: string
  settings: any
  status: string
  statusDetails: string
}

export interface IntegrationStatusResponse {
  count: number
  limit: number
  offset: number
  rows: IntegrationStatus[]
}

export interface GlobalIntegrationStatusCount {
  status: string
  count: number
}

export interface DashboardMetrics {
  activitiesTotal: number
  activitiesLast30Days: number
  organizationsTotal: number
  organizationsLast30Days: number
  membersTotal: number
  membersLast30Days: number
  projectsTotal: number
  projectsLast30Days: number
}
