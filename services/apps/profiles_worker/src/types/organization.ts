export interface IOrganizationProfileSyncInput {
  organization: {
    id: string
  }
  syncOptions?: IOrganizationSyncOptions
  recalculateAffiliations?: boolean
  afterMemberId?: string
}

export interface IOrganizationSyncOptions {
  doSync: boolean
  withAggs: boolean
}

export interface FakeOrganizationAnalysisInput {
  organizationId: string
}

export type FakeOrganizationVerdict = 'fake' | 'genuine' | 'unsure'
