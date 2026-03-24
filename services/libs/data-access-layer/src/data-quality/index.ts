import { IMember } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

/**
 * Fetches members who do not have any work experience.
 *
 * @param {QueryExecutor} qx - The query executor for running SQL queries.
 * @param {number} limit - The maximum number of members to return.
 * @param {number} offset - The number of members to skip before starting to collect the result set.
 * @param {string} segmentId - The segment ID to filter members by.
 * @return {Promise<IMember[]>} A promise that resolves to an array of members without work experience.
 */
export async function fetchMembersWithoutWorkExperience(
  qx: QueryExecutor,
  limit: number,
  offset: number,
  segmentId: string,
): Promise<IMember[]> {
  return qx.select(
    `
            SELECT m.id, m."displayName", m.attributes, msa."activityCount"
            FROM members m
                     LEFT JOIN "memberOrganizations" mo ON m.id = mo."memberId" AND mo."deletedAt" IS NULL
                     INNER JOIN "memberSegmentsAgg" msa ON m.id = msa."memberId" AND msa."segmentId" = '${segmentId}'
            WHERE mo."memberId" IS NULL
              AND m."deletedAt" IS NULL
              AND COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE) = FALSE
            ORDER BY msa."activityCount" DESC
            LIMIT ${limit} OFFSET ${offset};
        `,
    {
      segmentId,
      limit,
      offset,
    },
  )
}

/**
 * Fetches members with a number of identities that exceed a specified threshold.
 *
 * @param {QueryExecutor} qx - The query executor to perform database operations.
 * @param {number} [threshold=15] - The threshold for the number of identities a member must exceed to be included in the results.
 * @param {number} limit - The maximum number of members to return.
 * @param {number} offset - The number of members to skip before starting to collect the result set.
 * @param {string} segmentId - The ID of the segment within which the activity count is considered.
 * @return {Promise<IMember[]>} A promise that resolves to an array of members who have more identities than the specified threshold.
 */
export async function fetchMembersWithTooManyIdentities(
  qx: QueryExecutor,
  threshold = 15,
  limit: number,
  offset: number,
  segmentId: string,
): Promise<IMember[]> {
  return qx.select(
    `
            SELECT
                mi."memberId",
                m."displayName",
                m."attributes",
                m.id,
                COUNT(DISTINCT CASE WHEN mi."type" = 'email' THEN mi."value"::text ELSE mi.id::text END) AS "identityCount",
                msa."activityCount"
            FROM "memberIdentities" mi
                     JOIN "members" m ON mi."memberId" = m.id
                     INNER JOIN "memberSegmentsAgg" msa ON m.id = msa."memberId" AND msa."segmentId" = '${segmentId}'
            WHERE COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE) = FALSE
              AND mi."deletedAt" is null
            GROUP BY mi."memberId", m."displayName", m."attributes", m.id, msa."activityCount"
            HAVING COUNT(DISTINCT CASE WHEN mi."type" = 'email' THEN mi."value"::text ELSE mi.id::text END) > ${threshold}
            ORDER BY msa."activityCount" DESC
            LIMIT ${limit} OFFSET ${offset};
        `,
    {
      threshold,
      limit,
      offset,
      segmentId,
    },
  )
}

/**
 * Fetches members with a number of verified identities per platform exceeding a specified threshold for a given tenant.
 *
 * @param {QueryExecutor} qx - The query executor to run the database queries.
 * @param {number} [threshold=1] - The minimum number of verified identities per platform to filter members by. Defaults to 1.
 * @param {string} tenantId - The ID of the tenant to filter members.
 * @param {number} limit - The maximum number of records to return.
 * @param {number} offset - The number of records to skip.
 * @param {string} segmentId - The segment ID to fetch the member activity count.
 * @return {Promise<IMember[]>} A promise that resolves to an array of members matching the criteria.
 */
export async function fetchMembersWithTooManyIdentitiesPerPlatform(
  qx: QueryExecutor,
  threshold = 1,
  limit: number,
  offset: number,
  segmentId: string,
): Promise<IMember[]> {
  return qx.select(
    `
            WITH platform_identities AS (
                SELECT
                    mi."memberId",
                    mi.platform,
                    COUNT(*) AS "identityCount"
                FROM "memberIdentities" mi
                WHERE mi.type = 'username'
                  AND mi.verified = true
                  AND mi.platform IN ('linkedin', 'github', 'twitter', 'lfx', 'slack', 'cvent', 'tnc', 'groupsio')
                  AND mi."deletedAt" is null
                GROUP BY mi."memberId", mi.platform
                HAVING COUNT(*) > ${threshold}
            ),
                 aggregated_platforms AS (
                     SELECT
                         p."memberId",
                         STRING_AGG(p.platform, ',') AS platforms
                     FROM platform_identities p
                     GROUP BY p."memberId"
                 )
            SELECT
                p."memberId",
                m."displayName",
                m."attributes",
                m.id,
                platforms,
                msa."activityCount"
            FROM aggregated_platforms p
                     JOIN "members" m ON p."memberId" = m.id
                     INNER JOIN "memberSegmentsAgg" msa ON m.id = msa."memberId"
            WHERE msa."segmentId" = '${segmentId}'
              AND COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE) = FALSE
            ORDER BY msa."activityCount" DESC
            LIMIT ${limit} OFFSET ${offset};
        `,
    {
      threshold,
      limit,
      offset,
      segmentId,
    },
  )
}

/**
 * Fetches members who have more than a specified number of verified email addresses.
 *
 * @param {QueryExecutor} qx - The query executor to run database queries.
 * @param {number} [threshold=3] - The threshold number of email addresses a member must exceed to be included.
 * @param {number} limit - The maximum number of members to retrieve.
 * @param {number} offset - The number of members to skip before starting to collect the result set.
 * @param {string} segmentId - The ID of the segment to which the members belong.
 * @return {Promise<IMember[]>} - A promise that resolves to an array of members who have more than the specified number of verified email addresses.
 */
export async function fetchMembersWithTooManyEmails(
  qx: QueryExecutor,
  threshold = 3,
  limit: number,
  offset: number,
  segmentId: string,
): Promise<IMember[]> {
  return qx.select(
    `
        SELECT
            mi."memberId",
            m."displayName",
            m."attributes",
            m.id,
            COUNT(DISTINCT mi.value) AS "identityCount",
            msa."activityCount"
        FROM "memberIdentities" mi
                 JOIN "members" m ON mi."memberId" = m.id
                 INNER JOIN "memberSegmentsAgg" msa ON m.id = msa."memberId" AND msa."segmentId" = '${segmentId}'
        WHERE mi.verified = true
          AND mi.type = 'email'
          AND mi."deletedAt" is null
          AND COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE) = FALSE
        GROUP BY mi."memberId", m."displayName", m."attributes", m.id, msa."activityCount"
        HAVING COUNT(DISTINCT mi.value) > ${threshold}
        ORDER BY msa."activityCount" DESC
        LIMIT ${limit} OFFSET ${offset};
        `,
    {
      threshold,
      limit,
      offset,
      segmentId,
    },
  )
}

/**
 * Fetches members with missing information on work experience.
 *
 * @param {QueryExecutor} qx - The query executor instance used to perform database operations.
 * @param {number} limit - The maximum number of members to retrieve.
 * @param {number} offset - The starting point in the list of members to retrieve.
 * @param {string} segmentId - The ID of the segment to filter members by.
 * @return {Promise<IMember[]>} A promise that resolves to an array of members
 * with missing period information on their work experience.
 */
export async function fetchMembersWithMissingInfoOnWorkExperience(
  qx: QueryExecutor,
  limit: number,
  offset: number,
  segmentId: string,
): Promise<IMember[]> {
  return qx.select(
    `
          SELECT
              m.id,
              m."displayName",
              m."attributes",
              msa."activityCount",
              COUNT(mo.id) AS "organizationsCount"
          FROM "members" m
                   JOIN "memberOrganizations" mo ON m.id = mo."memberId" AND mo."deletedAt" IS NULL
                   INNER JOIN "memberSegmentsAgg" msa ON m.id = msa."memberId" AND msa."segmentId" = '${segmentId}'
          WHERE (mo."title" IS NULL OR mo."title"='')
            AND COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE) = FALSE
          GROUP BY m.id, msa."activityCount"
          ORDER BY msa."activityCount" DESC
          LIMIT ${limit} OFFSET ${offset};
      `,
    {
      limit,
      offset,
      segmentId,
    },
  )
}

/**
 * Fetches members with missing information on work experience.
 *
 * @param {QueryExecutor} qx - The query executor instance used to perform database operations.
 * @param {number} limit - The maximum number of members to retrieve.
 * @param {number} offset - The starting point in the list of members to retrieve.
 * @param {string} segmentId - The ID of the segment to filter members by.
 * @return {Promise<IMember[]>} A promise that resolves to an array of members
 * with missing period information on their work experience.
 */
export async function fetchMembersWithMissingPeriodOnWorkExperience(
  qx: QueryExecutor,
  limit: number,
  offset: number,
  segmentId: string,
): Promise<IMember[]> {
  return qx.select(
    `
          SELECT
              m.id,
              m."displayName",
              m."attributes",
              msa."activityCount",
              COUNT(mo.id) AS "organizationsCount"
          FROM "members" m
                   JOIN "memberOrganizations" mo ON m.id = mo."memberId" AND mo."deletedAt" IS NULL
                   INNER JOIN "memberSegmentsAgg" msa ON m.id = msa."memberId" AND msa."segmentId" = '${segmentId}'
          WHERE (mo."dateStart" IS NULL)
            AND COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE) = FALSE
          GROUP BY m.id, msa."activityCount"
          ORDER BY msa."activityCount" DESC
          LIMIT ${limit} OFFSET ${offset};
      `,
    {
      limit,
      offset,
      segmentId,
    },
  )
}

export async function fetchMembersWithConflictingWorkExperiences(
  qx: QueryExecutor,
  limit: number,
  offset: number,
  segmentId: string,
): Promise<IMember[]> {
  return qx.select(
    `
        SELECT
            m.id,
            m."displayName",
            m."attributes",
            msa."activityCount",
            COUNT(DISTINCT mo1.id) AS "organizationsCount"
        FROM "members" m
                JOIN "memberOrganizations" mo1 ON m.id = mo1."memberId" AND mo1."deletedAt" IS NULL
                INNER JOIN "memberSegmentsAgg" msa ON m.id = msa."memberId" AND msa."segmentId" = '${segmentId}'
                INNER JOIN "memberOrganizations" mo2 ON mo1."memberId" = mo2."memberId" AND mo2."deletedAt" IS NULL
                AND mo1.id != mo2.id
                AND (
                   (mo1."dateStart" < COALESCE(mo2."dateEnd", 'infinity'::timestamp)
                       AND COALESCE(mo1."dateEnd", 'infinity'::timestamp) > mo2."dateStart") OR
                   (mo2."dateStart" < COALESCE(mo1."dateEnd", 'infinity'::timestamp)
                       AND COALESCE(mo2."dateEnd", 'infinity'::timestamp) > mo1."dateStart")
               )
        WHERE COALESCE((m.attributes -> 'isBot' ->> 'default')::BOOLEAN, FALSE) = FALSE
        GROUP BY m.id, msa."activityCount"
        ORDER BY msa."activityCount" DESC
        LIMIT ${limit} OFFSET ${offset};
      `,
    {
      limit,
      offset,
      segmentId,
    },
  )
}
