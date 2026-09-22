import { QueryExecutor } from '../queryExecutor'
import { getSubProjectsCount } from '../segments'
import { IDashboardMetrics } from './types'

// Helper function to safely cast values to number
function toNumber(value: unknown): number {
  return Number(value) || 0
}

export async function getMetrics(
  qx: QueryExecutor,
  segmentId?: string,
): Promise<IDashboardMetrics> {
  const [snapshotData, projectsData] = await Promise.all([
    getSnapshotMetrics(qx, segmentId),
    getSubProjectsCount(qx, segmentId),
  ])

  return {
    activitiesLast30Days: toNumber(snapshotData?.activitiesLast30Days),
    activitiesTotal: toNumber(snapshotData?.activitiesTotal),
    membersLast30Days: toNumber(snapshotData?.membersLast30Days),
    membersTotal: toNumber(snapshotData?.membersTotal),
    organizationsLast30Days: toNumber(snapshotData?.organizationsLast30Days),
    organizationsTotal: toNumber(snapshotData?.organizationsTotal),
    projectsLast30Days: toNumber(projectsData.projectsLast30Days),
    projectsTotal: toNumber(projectsData.projectsTotal),
  }
}

async function getSnapshotMetrics(
  qx: QueryExecutor,
  segmentId?: string,
): Promise<Omit<IDashboardMetrics, 'projectsTotal' | 'projectsLast30Days'> | null> {
  const tableName = segmentId
    ? 'dashboardMetricsPerSegmentSnapshot'
    : 'dashboardMetricsTotalSnapshot'

  const query = segmentId
    ? `
      SELECT *
      FROM "${tableName}"
      WHERE "segmentId" = $(segmentId)
      LIMIT 1
    `
    : `
      SELECT 
        "activitiesLast30Days",
        "activitiesTotal",
        "membersLast30Days",
        "membersTotal",
        "organizationsLast30Days",
        "organizationsTotal",
        "updatedAt"
      FROM "${tableName}"
      LIMIT 1
    `

  const params = segmentId ? { segmentId } : {}
  const [row] = await qx.select(query, params)

  return row || null
}
