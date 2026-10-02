import { IPccCandidate } from './requestResolver'

const MAX_PCC_CANDIDATES = 5
const SNOWFLAKE_MAX_SCORE = 100

export const PCC_CANDIDATES_QUERY = `
  SELECT
    p.PROJECT_ID,
    p.NAME,
    p.SLUG,
    GREATEST(
      JAROWINKLER_SIMILARITY(LOWER(p.NAME), ?),
      JAROWINKLER_SIMILARITY(LOWER(p.SLUG), ?)
    ) AS SCORE,
    parents.PARENT_ID IS NULL AS IS_LEAF
  FROM ANALYTICS.SILVER_DIM.PROJECTS p
  LEFT JOIN (
    SELECT DISTINCT PARENT_ID
    FROM ANALYTICS.SILVER_DIM.PROJECTS
    WHERE PARENT_ID IS NOT NULL
  ) parents ON parents.PARENT_ID = p.PROJECT_ID
  WHERE NOT p.IS_INTERNAL_PROJECT
    AND p.NAME IS NOT NULL
    AND p.SLUG IS NOT NULL
  ORDER BY SCORE DESC, IS_LEAF DESC, p.PROJECT_ID
  LIMIT ${MAX_PCC_CANDIDATES}
`

export interface IPccCandidateRow {
  PROJECT_ID: string
  NAME: string
  SLUG: string
  SCORE: number
  IS_LEAF: boolean
}

export type PccQueryRunner = (query: string, binds: string[]) => Promise<IPccCandidateRow[]>

function toCandidate(row: IPccCandidateRow): IPccCandidate {
  return {
    projectId: row.PROJECT_ID,
    name: row.NAME,
    slug: row.SLUG,
    score: Number(row.SCORE) / SNOWFLAKE_MAX_SCORE,
    isLeaf: row.IS_LEAF,
  }
}

export function createPccCandidatesLookup(
  runQuery: PccQueryRunner,
): (projectName: string) => Promise<IPccCandidate[]> {
  return async (projectName) => {
    const normalized = projectName.trim().toLowerCase()
    const rows = await runQuery(PCC_CANDIDATES_QUERY, [normalized, normalized])
    return rows.map(toCandidate)
  }
}
