import { DEFAULT_TENANT_ID, generateUUIDv4 } from '@crowd/common'
import { IntegrationState, PlatformType } from '@crowd/types'

import { QueryExecutor } from '../queryExecutor'

export interface IFoundCredlyIntegration {
  id: string
  created: boolean
  restored: boolean
}

// integrations has no unique key on (platform, segmentId), so every writer of
// Credly integration rows serializes on this lock to avoid duplicates.
async function lockCredlyIntegrations(tx: QueryExecutor): Promise<void> {
  await tx.select(`SELECT pg_advisory_xact_lock(hashtextextended('credlyIntegration', 0))`)
}

async function insertCredlyIntegration(tx: QueryExecutor, segmentId: string): Promise<string> {
  const id = generateUUIDv4()
  await tx.result(
    `
      INSERT INTO public.integrations
        (id, platform, status, "segmentId", "tenantId", "createdAt", "updatedAt")
      VALUES ($(id), $(platform), $(status), $(segmentId), $(tenantId), now(), now())
    `,
    {
      id,
      platform: PlatformType.CREDLY,
      status: IntegrationState.DONE,
      segmentId,
      tenantId: DEFAULT_TENANT_ID,
    },
  )
  return id
}

export async function findOrCreateCredlyIntegration(
  qx: QueryExecutor,
  segmentId: string,
): Promise<IFoundCredlyIntegration> {
  return qx.tx(async (tx) => {
    await lockCredlyIntegrations(tx)

    const rows: { id: string; deletedAt: Date | null }[] = await tx.select(
      `
        SELECT i.id, i."deletedAt"
        FROM public.integrations i
        WHERE i.platform = $(platform) AND i."segmentId" = $(segmentId)
        ORDER BY i."updatedAt" DESC, i."createdAt" DESC, i.id
      `,
      { platform: PlatformType.CREDLY, segmentId },
    )

    const live = rows.filter((row) => row.deletedAt === null)
    if (live.length > 1) {
      throw new Error(`Found ${live.length} live Credly integrations for segment ${segmentId}!`)
    }
    if (live.length === 1) {
      return { id: live[0].id, created: false, restored: false }
    }

    if (rows.length > 0) {
      const { id } = rows[0]
      await tx.result(
        `UPDATE public.integrations SET "deletedAt" = NULL, "updatedAt" = now() WHERE id = $(id)`,
        { id },
      )
      return { id, created: false, restored: true }
    }

    const id = await insertCredlyIntegration(tx, segmentId)
    return { id, created: true, restored: false }
  })
}

export interface ICredlyTemplateMapping {
  badgeTemplateId: string
  badgeTemplateName: string
  integrationId: string
  segmentId: string
}

export type CredlyMappingStatus = 'mapped' | 'held' | 'excluded'
export type CredlyHoldReason = 'no_segment' | 'no_project' | 'ambiguous' | 'no_match' | 'test'

export interface ICredlyTemplateDecision {
  badgeTemplateId: string
  badgeTemplateName: string
  badgeCount?: number | null
  status: CredlyMappingStatus
  holdReason?: CredlyHoldReason | null
  projectSlug?: string | null
  segmentId?: string | null
  integrationId?: string | null
  rule?: string | null
  ruleVersion?: number | null
  matchedSku?: string | null
  evidence?: Record<string, unknown> | null
  reviewedBy?: string | null
}

export type CredlyDecisionOutcome = 'inserted' | 'updated' | 'unchanged' | 'locked'

export interface ICredlyHeldTemplate {
  badgeTemplateId: string
  badgeTemplateName: string
  badgeCount: number | null
  holdReason: CredlyHoldReason
  projectSlug: string | null
  rule: string | null
  lastSeenAt: Date
}

interface IStoredDecision {
  badgeTemplateName: string
  status: CredlyMappingStatus
  holdReason: CredlyHoldReason | null
  projectSlug: string | null
  segmentId: string | null
  integrationId: string | null
  rule: string | null
  ruleVersion: number | null
  matchedSku: string | null
  evidence: Record<string, unknown> | null
  reviewedBy: string | null
  lockedAt: Date | null
  deactivatedAt: Date | null
}

const DECISION_FIELDS = [
  'badgeTemplateName',
  'status',
  'holdReason',
  'projectSlug',
  'segmentId',
  'integrationId',
  'rule',
  'ruleVersion',
  'matchedSku',
  'reviewedBy',
] as const

function assertConsistent(decision: ICredlyTemplateDecision): void {
  if (decision.status === 'mapped' && !(decision.segmentId && decision.integrationId)) {
    throw new Error(
      `mapped template ${decision.badgeTemplateId} needs a segment and an integration`,
    )
  }
  if (decision.status === 'held' && !decision.holdReason) {
    throw new Error(`held template ${decision.badgeTemplateId} needs a hold reason`)
  }
}

function normalizedDecision(decision: ICredlyTemplateDecision): IStoredDecision {
  const mapped = decision.status === 'mapped'
  return {
    badgeTemplateName: decision.badgeTemplateName,
    status: decision.status,
    holdReason: decision.status === 'held' ? decision.holdReason : null,
    projectSlug: decision.projectSlug ?? null,
    segmentId: mapped ? (decision.segmentId ?? null) : null,
    integrationId: mapped ? (decision.integrationId ?? null) : null,
    rule: decision.rule ?? null,
    ruleVersion: decision.ruleVersion ?? null,
    matchedSku: decision.matchedSku ?? null,
    evidence: decision.evidence ?? null,
    reviewedBy: decision.reviewedBy ?? null,
    lockedAt: null,
    deactivatedAt: null,
  }
}

function targetChanged(stored: IStoredDecision, next: IStoredDecision): boolean {
  return (
    stored.status !== next.status ||
    stored.segmentId !== next.segmentId ||
    stored.integrationId !== next.integrationId
  )
}

// JSONB does not keep key order, so the stored evidence is compared with its keys sorted.
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => a.localeCompare(b))
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(',')}}`
  }
  return JSON.stringify(value) ?? 'null'
}

function decisionChanged(stored: IStoredDecision, next: IStoredDecision): boolean {
  return (
    stored.deactivatedAt !== null ||
    DECISION_FIELDS.some((field) => stored[field] !== next[field]) ||
    canonicalJson(stored.evidence) !== canonicalJson(next.evidence)
  )
}

// The same comparison the upsert makes, so a dry run predicts its outcome.
export function credlyDecisionChanged(
  stored: ICredlyStoredTemplateMapping,
  next: ICredlyTemplateDecision,
): boolean {
  return decisionChanged(stored, normalizedDecision(next))
}

async function assertLiveCredlyIntegration(
  qx: QueryExecutor,
  integrationId: string | null,
  segmentId: string | null,
): Promise<void> {
  const row = integrationId
    ? await qx.selectOneOrNone(
        `
          SELECT 1
          FROM public.integrations i
          WHERE i.id = $(integrationId)
            AND i."segmentId" = $(segmentId)
            AND i.platform = $(platform)
            AND i."deletedAt" IS NULL
        `,
        { integrationId, segmentId, platform: PlatformType.CREDLY },
      )
    : null
  if (!row) {
    throw new Error(
      `integration ${integrationId} is not a live Credly integration on segment ${segmentId}`,
    )
  }
}

async function touchDecision(
  qx: QueryExecutor,
  badgeTemplateId: string,
  badgeCount: number | null,
): Promise<void> {
  await qx.result(
    `
      UPDATE integration."credlyTemplateMappings"
      SET "lastSeenAt" = now(), "badgeCount" = COALESCE($(badgeCount), "badgeCount")
      WHERE "badgeTemplateId" = $(badgeTemplateId)
    `,
    { badgeTemplateId, badgeCount },
  )
}

// A locked template has emitted badges on its segment: the resolver may not move it, it only
// reports the difference. Everything else is rewritten when the decision changed.
export async function upsertCredlyTemplateDecision(
  qx: QueryExecutor,
  decision: ICredlyTemplateDecision,
): Promise<CredlyDecisionOutcome> {
  assertConsistent(decision)
  const next = normalizedDecision(decision)
  const values = {
    ...next,
    badgeTemplateId: decision.badgeTemplateId,
    badgeCount: decision.badgeCount ?? null,
    evidence: next.evidence ? JSON.stringify(next.evidence) : null,
  }

  return qx.tx(async (tx) => {
    await tx.select(
      `SELECT pg_advisory_xact_lock(hashtextextended('credlyTemplateMapping:' || $(badgeTemplateId), 0))`,
      { badgeTemplateId: decision.badgeTemplateId },
    )
    if (next.status === 'mapped') {
      await assertLiveCredlyIntegration(tx, next.integrationId, next.segmentId)
    }
    const stored: IStoredDecision | null = await tx.selectOneOrNone(
      `
        SELECT "badgeTemplateName", status, "holdReason", "projectSlug", "segmentId",
               "integrationId", rule, "ruleVersion", "matchedSku", evidence, "reviewedBy",
               "lockedAt", "deactivatedAt"
        FROM integration."credlyTemplateMappings"
        WHERE "badgeTemplateId" = $(badgeTemplateId)
        FOR UPDATE
      `,
      { badgeTemplateId: decision.badgeTemplateId },
    )

    if (!stored) {
      await tx.result(
        `
          INSERT INTO integration."credlyTemplateMappings"
            ("badgeTemplateId", "badgeTemplateName", "badgeCount", status, "holdReason",
             "projectSlug", "segmentId", "integrationId", rule, "ruleVersion", "matchedSku",
             evidence, "reviewedBy", "reviewedAt", "lastSeenAt")
          VALUES
            ($(badgeTemplateId), $(badgeTemplateName), $(badgeCount), $(status), $(holdReason),
             $(projectSlug), $(segmentId), $(integrationId), $(rule), $(ruleVersion), $(matchedSku),
             $(evidence)::jsonb, $(reviewedBy),
             CASE WHEN $(reviewedBy) IS NULL THEN NULL ELSE now() END, now())
        `,
        values,
      )
      return 'inserted'
    }

    if (stored.lockedAt !== null && targetChanged(stored, next)) {
      await touchDecision(tx, decision.badgeTemplateId, values.badgeCount)
      return 'locked'
    }

    if (!decisionChanged(stored, next)) {
      await touchDecision(tx, decision.badgeTemplateId, values.badgeCount)
      return 'unchanged'
    }

    await tx.result(
      `
        UPDATE integration."credlyTemplateMappings"
        SET "badgeTemplateName" = $(badgeTemplateName),
            "badgeCount" = COALESCE($(badgeCount), "badgeCount"),
            status = $(status),
            "holdReason" = $(holdReason),
            "projectSlug" = $(projectSlug),
            "segmentId" = $(segmentId),
            "integrationId" = $(integrationId),
            rule = $(rule),
            "ruleVersion" = $(ruleVersion),
            "matchedSku" = $(matchedSku),
            evidence = $(evidence)::jsonb,
            "reviewedBy" = $(reviewedBy),
            "reviewedAt" = CASE WHEN $(reviewedBy) IS NULL THEN NULL ELSE now() END,
            "deactivatedAt" = NULL,
            "lastSeenAt" = now(),
            "updatedAt" = now()
        WHERE "badgeTemplateId" = $(badgeTemplateId)
      `,
      values,
    )
    return 'updated'
  })
}

export async function lockCredlyTemplateMapping(
  qx: QueryExecutor,
  badgeTemplateId: string,
): Promise<boolean> {
  const locked = await qx.result(
    `
      UPDATE integration."credlyTemplateMappings"
      SET "lockedAt" = now()
      WHERE "badgeTemplateId" = $(badgeTemplateId)
        AND status = 'mapped'
        AND "deactivatedAt" IS NULL
        AND "lockedAt" IS NULL
    `,
    { badgeTemplateId },
  )
  return locked > 0
}

export async function listActiveCredlyTemplateMappings(
  qx: QueryExecutor,
): Promise<ICredlyTemplateMapping[]> {
  return qx.select(
    `
      SELECT m."badgeTemplateId", m."badgeTemplateName", m."integrationId", m."segmentId"
      FROM integration."credlyTemplateMappings" m
      JOIN public.integrations i ON i.id = m."integrationId"
      WHERE m.status = 'mapped'
        AND m."deactivatedAt" IS NULL
        AND i.platform = $(platform)
        AND i."deletedAt" IS NULL
        AND i."segmentId" = m."segmentId"
      ORDER BY m."badgeTemplateId"
    `,
    { platform: PlatformType.CREDLY },
  )
}

export async function listStaleCredlyTemplateMappings(
  qx: QueryExecutor,
): Promise<ICredlyTemplateMapping[]> {
  return qx.select(
    `
      SELECT m."badgeTemplateId", m."badgeTemplateName", m."integrationId", m."segmentId"
      FROM integration."credlyTemplateMappings" m
      LEFT JOIN public.integrations i
        ON i.id = m."integrationId"
       AND i.platform = $(platform)
       AND i."deletedAt" IS NULL
       AND i."segmentId" = m."segmentId"
      WHERE m.status = 'mapped' AND m."deactivatedAt" IS NULL AND i.id IS NULL
      ORDER BY m."badgeTemplateId"
    `,
    { platform: PlatformType.CREDLY },
  )
}

export type ICredlyStoredTemplateMapping = Required<
  Pick<
    ICredlyTemplateDecision,
    | 'badgeTemplateId'
    | 'badgeTemplateName'
    | 'status'
    | 'holdReason'
    | 'projectSlug'
    | 'segmentId'
    | 'integrationId'
    | 'rule'
    | 'ruleVersion'
    | 'matchedSku'
    | 'evidence'
    | 'reviewedBy'
  >
> & { lockedAt: Date | null; deactivatedAt: Date | null }

export async function listCredlyTemplateMappings(
  qx: QueryExecutor,
): Promise<ICredlyStoredTemplateMapping[]> {
  return qx.select(
    `
      SELECT "badgeTemplateId", "badgeTemplateName", status, "holdReason", "projectSlug",
             "segmentId", "integrationId", rule, "ruleVersion", "matchedSku", evidence,
             "reviewedBy", "lockedAt", "deactivatedAt"
      FROM integration."credlyTemplateMappings"
      ORDER BY "badgeTemplateId"
    `,
  )
}

export async function listHeldCredlyTemplateMappings(
  qx: QueryExecutor,
): Promise<ICredlyHeldTemplate[]> {
  return qx.select(
    `
      SELECT "badgeTemplateId", "badgeTemplateName", "badgeCount", "holdReason", "projectSlug",
             rule, "lastSeenAt"
      FROM integration."credlyTemplateMappings"
      WHERE status = 'held' AND "deactivatedAt" IS NULL
      ORDER BY "badgeCount" DESC NULLS LAST, "badgeTemplateName"
    `,
  )
}

export async function deactivateCredlyTemplateMapping(
  qx: QueryExecutor,
  badgeTemplateId: string,
): Promise<number> {
  return qx.result(
    `
      UPDATE integration."credlyTemplateMappings"
      SET "deactivatedAt" = now(), "updatedAt" = now()
      WHERE "badgeTemplateId" = $(badgeTemplateId)
        AND "deactivatedAt" IS NULL
        AND "lockedAt" IS NULL
    `,
    { badgeTemplateId },
  )
}
