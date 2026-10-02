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
    ) AS SCORE
  FROM ANALYTICS.SILVER_DIM.PROJECTS
  WHERE NOT IS_INTERNAL_PROJECT
  ORDER BY SCORE DESC
  LIMIT ${MAX_PCC_CANDIDATES}
`

export interface IPccCandidateRow {
  PROJECT_ID: string
  NAME: string
  SLUG: string
  SCORE: number
}

export type PccQueryRunner = (query: string, binds: string[]) => Promise<IPccCandidateRow[]>

function toCandidate(row: IPccCandidateRow): IPccCandidate {
  return {
    projectId: row.PROJECT_ID,
    name: row.NAME,
    slug: row.SLUG,
    score: Number(row.SCORE) / SNOWFLAKE_MAX_SCORE,
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
