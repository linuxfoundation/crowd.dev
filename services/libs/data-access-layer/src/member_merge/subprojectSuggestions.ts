import { QueryExecutor } from '../queryExecutor'

type SubprojectMember = {
  id: string
  displayName: string
  activityCount: number
  identities: {
    platform: string
    type: string
    value: string
    verified: boolean
  }[]
}

type SubprojectMemberMergePair = {
  primary: SubprojectMember
  other: SubprojectMember
}

export async function fetchRecentlyOnboardedSubprojects(qx: QueryExecutor): Promise<string[]> {
  const rows = await qx.select(
    `
      SELECT ip."segmentId"
      FROM "insightsProjects" ip
      JOIN integrations i
        ON i."segmentId" = ip."segmentId"
       AND i.status = 'done'
       AND i."deletedAt" IS NULL
      WHERE ip."deletedAt" IS NULL
        AND ip.enabled
        AND ip."segmentId" IS NOT NULL
        AND ip."createdAt" >= now() - interval '7 days'
        AND EXISTS (
          SELECT 1
          FROM "memberSegmentsAgg" msa
          WHERE msa."segmentId" = ip."segmentId"
        )
      GROUP BY ip."segmentId"
      HAVING count(*) >= 2
    `,
  )

  return rows.map((row) => row.segmentId)
}

export async function fetchSubprojectMemberMergePairs(
  qx: QueryExecutor,
  segmentId: string,
): Promise<SubprojectMemberMergePair[]> {
  return qx.select(
    `
      WITH project_members AS MATERIALIZED (
        SELECT
          msa."memberId",
          msa."activityCount"::int AS "activityCount",
          m."displayName",
          lower(regexp_replace(trim(m."displayName"), '\\s+', ' ', 'g')) AS norm_name,
          lower(regexp_replace(m."displayName", '[^a-zA-Z0-9]+', '', 'g')) AS compact_name,
          array_to_string(
            ARRAY(
              SELECT unnest(string_to_array(
                lower(regexp_replace(trim(m."displayName"), '\\s+', ' ', 'g')),
                ' '
              ))
              ORDER BY 1
            ),
            ' '
          ) AS sorted_tokens
        FROM "memberSegmentsAgg" msa
        JOIN members m ON m.id = msa."memberId"
        WHERE msa."segmentId" = $(segmentId)
          AND m."deletedAt" IS NULL
          AND COALESCE((m.attributes->'isBot'->>'default')::boolean, false) IS FALSE
          AND COALESCE((m.attributes->'isOrganization'->>'default')::boolean, false) IS FALSE
      ),
      project_identities AS MATERIALIZED (
        SELECT
          mi."memberId",
          mi.platform,
          mi.type,
          mi.verified,
          lower(mi.value) AS val,
          CASE
            WHEN position('@' IN mi.value) > 0 THEN split_part(lower(mi.value), '@', 1)
            ELSE NULL
          END AS local_part
        FROM "memberIdentities" mi
        JOIN project_members pm ON pm."memberId" = mi."memberId"
        WHERE mi."deletedAt" IS NULL
      ),
      usernames AS MATERIALIZED (
        SELECT "memberId", val
        FROM project_identities
        WHERE type = 'username'
          AND platform <> 'git'
          AND position('@' IN val) = 0
          AND length(val) >= 5
      ),
      keyed AS (
        SELECT 'same_display_name'::text AS rule, pm.norm_name AS key, pm.norm_name, pm."memberId", pm."activityCount"
        FROM project_members pm
        WHERE length(pm.norm_name) >= 4
          AND pm.norm_name NOT IN ('unknown', 'root', 'ubuntu', 'admin', 'user', 'guest')
        UNION
        SELECT 'reversed_name', pm.sorted_tokens, pm.norm_name, pm."memberId", pm."activityCount"
        FROM project_members pm
        WHERE pm.norm_name LIKE '% %'
        UNION
        SELECT 'display_name_username', u.val, pm.norm_name, u."memberId", pm."activityCount"
        FROM usernames u
        JOIN project_members pm ON pm."memberId" = u."memberId"
        UNION
        SELECT 'display_name_username', pm.norm_name, pm.norm_name, pm."memberId", pm."activityCount"
        FROM project_members pm
        JOIN usernames u ON u.val = pm.norm_name AND u."memberId" <> pm."memberId"
        WHERE length(pm.compact_name) >= 5
        UNION
        SELECT 'display_name_username', pm.compact_name, pm.norm_name, pm."memberId", pm."activityCount"
        FROM project_members pm
        JOIN usernames u ON u.val = pm.compact_name AND u."memberId" <> pm."memberId"
        WHERE length(pm.compact_name) >= 5
          AND pm.compact_name <> pm.norm_name
        UNION
        SELECT 'email_localpart_username', u.val, pm.norm_name, u."memberId", pm."activityCount"
        FROM usernames u
        JOIN project_members pm ON pm."memberId" = u."memberId"
        UNION
        SELECT 'email_localpart_username', ea.local_part, pm.norm_name, ea."memberId", pm."activityCount"
        FROM project_identities ea
        JOIN project_members pm ON pm."memberId" = ea."memberId"
        JOIN usernames u ON u.val = ea.local_part AND u."memberId" <> ea."memberId"
        WHERE ea.local_part IS NOT NULL
          AND length(ea.local_part) >= 5
      ),
      ranked AS (
        SELECT
          k.*,
          ROW_NUMBER() OVER (
            PARTITION BY k.rule, k.key
            ORDER BY k."activityCount" DESC, k."memberId"
          ) AS rn
        FROM keyed k
      ),
      match_keys AS (
        SELECT rule, key
        FROM keyed
        GROUP BY rule, key
        HAVING count(*) >= 2
          AND (rule <> 'reversed_name' OR count(DISTINCT norm_name) >= 2)
      ),
      stars AS (
        SELECT
          p."memberId" AS primary_id,
          o."memberId" AS other_id
        FROM ranked p
        JOIN match_keys g ON g.rule = p.rule AND g.key = p.key
        JOIN ranked o
          ON o.rule = p.rule
         AND o.key = p.key
         AND o."memberId" <> p."memberId"
        WHERE p.rn = 1
      ),
      pairs AS (
        SELECT primary_id, other_id
        FROM stars
        GROUP BY 1, 2
      ),
      pair_members AS (
        SELECT primary_id AS "memberId" FROM pairs
        UNION
        SELECT other_id FROM pairs
      ),
      member_identities AS MATERIALIZED (
        SELECT
          mi."memberId",
          jsonb_agg(
            DISTINCT jsonb_build_object(
              'platform', mi.platform,
              'type', mi.type,
              'value', mi.value,
              'verified', mi.verified
            )
          ) AS identities
        FROM "memberIdentities" mi
        JOIN pair_members pm ON pm."memberId" = mi."memberId"
        WHERE mi."deletedAt" IS NULL
        GROUP BY mi."memberId"
      )
      SELECT
        jsonb_build_object(
          'id', prim."memberId",
          'displayName', prim."displayName",
          'activityCount', prim."activityCount",
          'identities', COALESCE(pi.identities, '[]'::jsonb)
        ) AS primary,
        jsonb_build_object(
          'id', oth."memberId",
          'displayName', oth."displayName",
          'activityCount', oth."activityCount",
          'identities', COALESCE(oi.identities, '[]'::jsonb)
        ) AS other
      FROM pairs p
      JOIN project_members prim ON prim."memberId" = p.primary_id
      JOIN project_members oth ON oth."memberId" = p.other_id
      LEFT JOIN member_identities pi ON pi."memberId" = p.primary_id
      LEFT JOIN member_identities oi ON oi."memberId" = p.other_id
      WHERE NOT EXISTS (
        SELECT 1
        FROM "memberNoMerge" nm
        WHERE (nm."memberId" = p.primary_id AND nm."noMergeId" = p.other_id)
           OR (nm."memberId" = p.other_id AND nm."noMergeId" = p.primary_id)
      )
      ORDER BY GREATEST(prim."activityCount", oth."activityCount") DESC, p.primary_id, p.other_id
    `,
    { segmentId },
  )
}
