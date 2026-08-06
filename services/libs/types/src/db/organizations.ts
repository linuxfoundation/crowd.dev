export interface OrganizationDbRow {
  id: string
  displayName: string | null
  description: string | null
  logo: string | null
  tags: string[] | null
  employees: number | null
  revenueRange: Record<string, unknown> | null
  importHash: string | null
  location: string | null
  isTeamOrganization: boolean
  isAffiliationBlocked: boolean
  type: string | null
  size: string | null
  headline: string | null
  industry: string | null
  founded: number | null
  employeeChurnRate: Record<string, unknown> | null
  employeeGrowthRate: Record<string, unknown> | null
  manuallyCreated: boolean
  lastEnrichedAt: string | null
  tenantId: string
  createdAt: string
  updatedAt: string
  deletedAt: string | null
  createdById: string | null
  updatedById: string | null
}

export type OrganizationDbInsert = Partial<
  Omit<OrganizationDbRow, 'tenantId' | 'createdAt' | 'updatedAt' | 'deletedAt' | 'lastEnrichedAt'>
>
