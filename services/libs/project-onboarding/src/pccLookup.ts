import { IPccCandidate } from './requestResolver'

const MAX_PCC_CANDIDATES = 5
const SNOWFLAKE_MAX_SCORE = 100

export const PCC_CANDIDATES_QUERY = `
  SELECT
    PROJECT_ID,
    NAME,
    SLUG,
    GREATEST(
      JAROWINKLER_SIMILARITY(LOWER(NAME), ?),
      JAROWINKLER_SIMILARITY(LOWER(SLUG), ?)
    ) AS SCORE,
    PROJECT_ID NOT IN (
      SELECT DISTINCT PARENT_ID
      FROM ANALYTICS.SILVER_DIM.PROJECTS
      WHERE PARENT_ID IS NOT NULL
    ) AS IS_LEAF
  FROM ANALYTICS.SILVER_DIM.PROJECTS
  WHERE NOT IS_INTERNAL_PROJECT
    AND NAME IS NOT NULL
    AND SLUG IS NOT NULL
  ORDER BY SCORE DESC, IS_LEAF DESC, PROJECT_ID
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
