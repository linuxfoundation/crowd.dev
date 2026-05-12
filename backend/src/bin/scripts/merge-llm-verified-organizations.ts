import commandLineArgs from 'command-line-args'

import { DEFAULT_TENANT_ID } from '@crowd/common'
import { QueryExecutor } from '@crowd/data-access-layer'
import { chunkArray } from '@crowd/data-access-layer/src/old/apps/merge_suggestions_worker/utils'
import { getServiceLogger } from '@crowd/logging'

import OrganizationService from '@/services/organizationService'

import SequelizeRepository from '../../database/repositories/sequelizeRepository'

const log = getServiceLogger()

interface LlmVerifiedOrganizationMergeCandidate {
  primaryId: string
  secondaryId: string
  mergeState: string | null
  mergeStep: string | null
  verdictCreatedAt: Date
  suggestionCreatedAt: Date
  primaryLfx: boolean
  secondaryLfx: boolean
  reorderedForLfx: boolean
}

const options = [
  {
    name: 'testRun',
    alias: 't',
    type: Boolean,
    description: 'Run in test mode (limit to 10 organization merge candidates).',
  },
  {
    name: 'batchSize',
    alias: 'b',
    type: Number,
    description: 'Number of candidates to fetch per batch.',
  },
  {
    name: 'parallelism',
    alias: 'p',
    type: Number,
    description: 'Number of organization merges to run at once. Defaults to 1.',
  },
  {
    name: 'help',
    alias: 'h',
    type: Boolean,
    description: 'Print this usage guide.',
  },
]

const parameters = commandLineArgs(options)

function selectIndependentCandidates(
  candidates: LlmVerifiedOrganizationMergeCandidate[],
): LlmVerifiedOrganizationMergeCandidate[] {
  const selectedOrganizationIds = new Set<string>()
  const selected: LlmVerifiedOrganizationMergeCandidate[] = []

  for (const candidate of candidates) {
    const isConflicting =
      selectedOrganizationIds.has(candidate.primaryId) ||
      selectedOrganizationIds.has(candidate.secondaryId)

    if (isConflicting) {
      log.info(
        {
          primaryId: candidate.primaryId,
          secondaryId: candidate.secondaryId,
        },
        'Skipping LLM verified organization merge because one org is already selected in this batch!',
      )
    } else {
      selectedOrganizationIds.add(candidate.primaryId)
      selectedOrganizationIds.add(candidate.secondaryId)
      selected.push(candidate)
    }
  }

  return selected
}

async function fetchLlmVerifiedOrganizationMergeCandidates(
  qx: QueryExecutor,
  batchSize: number,
  excludedOrganizationIds: string[],
): Promise<LlmVerifiedOrganizationMergeCandidate[]> {
  const excludedOrganizationFilter =
    excludedOrganizationIds.length > 0
      ? `
        AND s."organizationId" NOT IN ($(excludedOrganizationIds:csv))
        AND s."toMergeId" NOT IN ($(excludedOrganizationIds:csv))
      `
      : ''

  return qx.select(
    `
    WITH stale_true_suggestions AS (
      SELECT
        s."organizationId",
        s."toMergeId",
        s.similarity,
        s."createdAt" AS "suggestionCreatedAt"
      FROM "organizationToMerge" s
      WHERE s.similarity > 0.75
        AND s."createdAt" < NOW() - INTERVAL '7 days'
        ${excludedOrganizationFilter}
    ),
    latest_true_verdict AS (
      SELECT DISTINCT ON (s."organizationId", s."toMergeId")
        s."organizationId",
        s."toMergeId",
        l."createdAt" AS "verdictCreatedAt"
      FROM stale_true_suggestions s
      JOIN "llmSuggestionVerdicts" l
        ON l.type = 'organization'
       AND l.verdict = 'true'
       AND (
            (l."primaryId" = s."organizationId" AND l."secondaryId" = s."toMergeId")
         OR (l."primaryId" = s."toMergeId" AND l."secondaryId" = s."organizationId")
       )
      ORDER BY s."organizationId", s."toMergeId", l."createdAt" DESC
    ),
    latest_pair_merge_action AS (
      SELECT DISTINCT ON (v."organizationId", v."toMergeId")
        v."organizationId",
        v."toMergeId",
        ma.state,
        ma.step,
        ma."createdAt"
      FROM latest_true_verdict v
      LEFT JOIN "mergeActions" ma
        ON ma.type = 'org'
       AND (
            (ma."primaryId" = v."organizationId" AND ma."secondaryId" = v."toMergeId")
         OR (ma."primaryId" = v."toMergeId" AND ma."secondaryId" = v."organizationId")
       )
      ORDER BY v."organizationId", v."toMergeId", ma."createdAt" DESC NULLS LAST
    ),
    candidates AS (
      SELECT
        v."organizationId",
        v."toMergeId",
        ma.state AS "mergeState",
        ma.step AS "mergeStep",
        v."verdictCreatedAt",
        s."suggestionCreatedAt",
        EXISTS (
          SELECT 1
          FROM "lfxMemberships" lm
          WHERE lm."organizationId" = v."organizationId"
        ) AS "primaryLfx",
        EXISTS (
          SELECT 1
          FROM "lfxMemberships" lm
          WHERE lm."organizationId" = v."toMergeId"
        ) AS "secondaryLfx"
      FROM latest_true_verdict v
      JOIN stale_true_suggestions s
        ON s."organizationId" = v."organizationId"
       AND s."toMergeId" = v."toMergeId"
      JOIN organizations o1 ON o1.id = v."organizationId"
      JOIN organizations o2 ON o2.id = v."toMergeId"
      LEFT JOIN latest_pair_merge_action ma
        ON ma."organizationId" = v."organizationId"
       AND ma."toMergeId" = v."toMergeId"
      WHERE (ma.state = 'error' OR ma.state IS NULL)
        AND o1."tenantId" = $(tenantId)
        AND o2."tenantId" = $(tenantId)
        AND NOT EXISTS (
          SELECT 1
          FROM "organizationNoMerge" n
          WHERE (n."organizationId" = v."organizationId" AND n."noMergeId" = v."toMergeId")
             OR (n."organizationId" = v."toMergeId" AND n."noMergeId" = v."organizationId")
        )
        AND NOT (
          EXISTS (
            SELECT 1
            FROM "lfxMemberships" lm
            WHERE lm."organizationId" = v."organizationId"
          )
          AND EXISTS (
            SELECT 1
            FROM "lfxMemberships" lm
            WHERE lm."organizationId" = v."toMergeId"
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM "mergeActions" ma
          WHERE ma.type = 'org'
            AND ma.state = 'in-progress'
            AND (
              ma."primaryId" IN (v."organizationId", v."toMergeId")
              OR ma."secondaryId" IN (v."organizationId", v."toMergeId")
            )
        )
        AND (
          ma.state = 'error'
          OR NOT EXISTS (
            SELECT 1
            FROM "mergeActions" ma
            WHERE ma.type = 'org'
              AND ma.state = 'merged'
              AND (
                ma."primaryId" IN (v."organizationId", v."toMergeId")
                OR ma."secondaryId" IN (v."organizationId", v."toMergeId")
              )
          )
        )
    )
    SELECT
      CASE
        WHEN c."secondaryLfx" AND NOT c."primaryLfx" THEN c."toMergeId"
        ELSE c."organizationId"
      END AS "primaryId",
      CASE
        WHEN c."secondaryLfx" AND NOT c."primaryLfx" THEN c."organizationId"
        ELSE c."toMergeId"
      END AS "secondaryId",
      c."mergeState",
      c."mergeStep",
      c."verdictCreatedAt",
      c."suggestionCreatedAt",
      c."primaryLfx",
      c."secondaryLfx",
      c."secondaryLfx" AND NOT c."primaryLfx" AS "reorderedForLfx"
    FROM candidates c
    ORDER BY c."verdictCreatedAt" ASC
    LIMIT $(batchSize);
    `,
    {
      batchSize,
      excludedOrganizationIds,
      tenantId: DEFAULT_TENANT_ID,
    },
  )
}

setImmediate(async () => {
  const testRun = parameters.testRun ?? false
  const BATCH_SIZE = parameters.batchSize ?? (testRun ? 10 : 100)
  const PARALLEL_MERGES = parameters.parallelism ?? 1

  const options = await SequelizeRepository.getDefaultIRepositoryOptions()
  const qx = SequelizeRepository.getQueryExecutor(options)
  const processedOrganizationIds = new Set<string>()

  log.info({ testRun, BATCH_SIZE, PARALLEL_MERGES }, 'Running script with the following parameters!')

  let candidates: LlmVerifiedOrganizationMergeCandidate[] = []

  do {
    candidates = await fetchLlmVerifiedOrganizationMergeCandidates(
      qx,
      BATCH_SIZE,
      Array.from(processedOrganizationIds),
    )

    log.info({ count: candidates.length }, 'Fetched LLM verified organization merge candidates!')

    const independentCandidates = selectIndependentCandidates(candidates)

    log.info(
      {
        count: independentCandidates.length,
        skipped: candidates.length - independentCandidates.length,
      },
      'Selected independent LLM verified organization merge candidates!',
    )

    const chunks = chunkArray(independentCandidates, PARALLEL_MERGES)

    for (const chunk of chunks) {
      await Promise.all(
        chunk.map(async (candidate) => {
          log.info(
            {
              primaryId: candidate.primaryId,
              secondaryId: candidate.secondaryId,
              reorderedForLfx: candidate.reorderedForLfx,
            },
            'Retrying LLM verified organization merge!',
          )

          try {
            const candidateOptions = {
              ...options,
              currentTenant: { id: DEFAULT_TENANT_ID },
            }
            const service = new OrganizationService(candidateOptions)

            await service.mergeSync(candidate.primaryId, candidate.secondaryId, null)

            processedOrganizationIds.add(candidate.primaryId)
            processedOrganizationIds.add(candidate.secondaryId)
          } catch (err) {
            log.error(
              {
                primaryId: candidate.primaryId,
                secondaryId: candidate.secondaryId,
                err,
              },
              'Failed retrying LLM verified organization merge!',
            )
            throw err
          }
        }),
      )
    }

    if (testRun) {
      log.info('Test run - stopping after first batch!')
      break
    }
  } while (candidates.length > 0)

  process.exit(0)
})
