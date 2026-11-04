import type { ConnectorHttp } from '@crowd/connectors'
import { fetchBadgeTemplatesPage } from '@crowd/connectors/src/connectors/credly/apiClient'
import type { Catalogue } from '@crowd/connectors/src/connectors/credly/mapping/catalogue'
import type {
  ResolvedTemplate,
  TemplateToResolve,
} from '@crowd/connectors/src/connectors/credly/mapping/resolver'
import { WINDOW_LAG_MS } from '@crowd/connectors/src/connectors/credly/syncs/badgePoll'
import { decommissionSyncUnitsForChannelIds } from '@crowd/data-access-layer/src/connectors'
import type {
  CredlyDecisionOutcome,
  ICredlyStoredTemplateMapping,
} from '@crowd/data-access-layer/src/credly'
import {
  credlyDecisionChanged,
  deactivateCredlyTemplateMapping,
  findOrCreateCredlyIntegration,
  lockCredlyTemplateMapping,
  upsertCredlyTemplateDecision,
} from '@crowd/data-access-layer/src/credly'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { listActiveSubprojectSegments } from '@crowd/data-access-layer/src/segments'
import type { Logger } from '@crowd/logging'

import { onboardCredlyTemplate } from './onboardCredlyTemplate'

export interface CatalogueSource {
  run<T>(query: string): Promise<T[]>
}

export async function loadCatalogue(source: CatalogueSource): Promise<Catalogue> {
  const [courses, courseGroups, enrollments] = await Promise.all([
    source.run<Catalogue['courses'][number]>(
      `SELECT sku AS "sku", title AS "title", course_group_id AS "courseGroupId",
              COALESCE(is_test_or_archived, FALSE) AS "isTestOrArchived"
       FROM bronze_census_ti.courses
       WHERE sku IS NOT NULL AND TRIM(sku) <> ''
       ORDER BY sku`,
    ),
    source.run<Catalogue['courseGroups'][number]>(
      `SELECT course_group_id AS "courseGroupId", course_group_title AS "title",
              project_slug AS "projectSlug"
       FROM silver_dim._ti_course_groups_to_projects
       WHERE project_slug IS NOT NULL AND TRIM(project_slug) <> ''
       ORDER BY course_group_id, project_slug`,
    ),
    source.run<Catalogue['enrollments'][number]>(
      `SELECT course_code AS "courseCode", project_slug AS "projectSlug", COUNT(*) AS "enrollments"
       FROM silver_fact.enrollments
       WHERE course_code IS NOT NULL AND project_slug IS NOT NULL
       GROUP BY 1, 2
       ORDER BY 1, 2`,
    ),
  ])
  return {
    courses,
    courseGroups,
    enrollments: enrollments.map((row) => ({ ...row, enrollments: Number(row.enrollments) })),
  }
}

export interface CredlyTemplateListing {
  templates: TemplateToResolve[]
  complete: boolean
}

// A listing that shrank while we paged it must not deactivate the templates we never saw.
export async function loadCredlyTemplates(
  http: ConnectorHttp,
  organizationId: string,
  log: Logger,
): Promise<CredlyTemplateListing> {
  const templates: TemplateToResolve[] = []
  const totals = new Set<number>()
  for (let page = 1; ; page += 1) {
    const result = await fetchBadgeTemplatesPage(http, { organizationId, page }, log)
    totals.add(result.metadata.total_count)
    for (const template of result.data) {
      const count = template.badges_count
      templates.push({
        id: template.id,
        name: template.name,
        badgeCount: typeof count === 'number' && Number.isFinite(count) ? count : null,
        url: template.url ?? null,
        createdAt: template.created_at ?? null,
      })
    }
    if (result.data.length === 0 || page >= result.metadata.total_pages) break
  }
  const ids = new Set(templates.map((template) => template.id))
  const [expected] = totals
  const complete =
    totals.size === 1 && ids.size === templates.length && templates.length === expected
  return { templates, complete }
}

// A slug shared by subprojects under different parents cannot be a target: it stays unmapped.
export async function loadSubprojectSegments(
  qx: QueryExecutor,
  log: Logger,
): Promise<Map<string, string>> {
  const idsBySlug = new Map<string, string[]>()
  for (const row of await listActiveSubprojectSegments(qx)) {
    const slug = row.slug.toLowerCase()
    idsBySlug.set(slug, [...(idsBySlug.get(slug) ?? []), row.id])
  }
  const duplicates = [...idsBySlug].filter(([, ids]) => ids.length > 1).map(([slug]) => slug)
  if (duplicates.length > 0) {
    log.warn({ duplicates }, 'subproject slugs shared by several segments are left unmapped')
  }
  return new Map(
    [...idsBySlug].filter(([, ids]) => ids.length === 1).map(([slug, ids]) => [slug, ids[0]]),
  )
}

export type PlanEntryKind = 'new' | 'unchanged' | 'changed' | 'locked' | 'missing'

export interface PlanEntry {
  kind: PlanEntryKind
  badgeTemplateId: string
  badgeTemplateName: string
  before: string | null
  after: string | null
}

export interface ResolvePlan {
  entries: PlanEntry[]
  counts: Record<PlanEntryKind, number>
}

function targetLabel(row: {
  status: string
  holdReason?: string | null
  projectSlug?: string | null
}): string {
  if (row.status === 'mapped') return `mapped:${row.projectSlug}`
  if (row.status === 'held') return `held:${row.holdReason}`
  return row.status
}

function names(row: { badgeTemplateId: string; badgeTemplateName: string }) {
  return { badgeTemplateId: row.badgeTemplateId, badgeTemplateName: row.badgeTemplateName }
}

function targetChanged(stored: ICredlyStoredTemplateMapping, next: ResolvedTemplate): boolean {
  return stored.status !== next.status || stored.segmentId !== next.segmentId
}

// The integration is assigned on apply: the plan keeps the stored one so it does not count as a change.
function decisionChanged(stored: ICredlyStoredTemplateMapping, next: ResolvedTemplate): boolean {
  const integrationId = targetChanged(stored, next) ? null : stored.integrationId
  return credlyDecisionChanged(stored, { ...next, integrationId })
}

export function buildPlan(
  decisions: ResolvedTemplate[],
  stored: ICredlyStoredTemplateMapping[],
): ResolvePlan {
  const storedById = new Map(stored.map((row) => [row.badgeTemplateId, row]))
  const entries: PlanEntry[] = decisions.map((decision) => {
    const row = storedById.get(decision.badgeTemplateId)
    storedById.delete(decision.badgeTemplateId)
    const after = targetLabel(decision)
    if (!row) return { kind: 'new', ...names(decision), before: null, after }
    const before = targetLabel(row)
    if (!decisionChanged(row, decision)) {
      return { kind: 'unchanged', ...names(decision), before, after }
    }
    const kind = row.lockedAt !== null && targetChanged(row, decision) ? 'locked' : 'changed'
    return { kind, ...names(decision), before, after }
  })
  for (const row of storedById.values()) {
    if (row.deactivatedAt !== null) continue
    const kind = row.lockedAt !== null ? 'locked' : 'missing'
    entries.push({ kind, ...names(row), before: targetLabel(row), after: null })
  }
  const counts: Record<PlanEntryKind, number> = {
    new: 0,
    unchanged: 0,
    changed: 0,
    locked: 0,
    missing: 0,
  }
  for (const entry of entries) counts[entry.kind] += 1
  return { entries, counts }
}

export interface ApplyResult {
  outcomes: Record<CredlyDecisionOutcome, number>
  integrationsCreated: number
  deactivated: number
  unitsOnboarded: number
  unitsDecommissioned: number
}

// Credly moved to its current platform in March 2019; nothing older carries a state_updated_at.
export const CREDLY_HISTORY_START = '2019-03-01T00:00:00.000Z'

export function releaseFloor(createdAt: string | null | undefined, top: Date): string {
  const createdMs = Date.parse(createdAt ?? '')
  const startMs = Date.parse(CREDLY_HISTORY_START)
  const floorMs = Number.isFinite(createdMs) ? Math.max(createdMs, startMs) : startMs
  return new Date(Math.min(floorMs, top.getTime() - 1000)).toISOString()
}

// The live cursor starts where the backfill's top ends, both clamped to WINDOW_LAG_MS before `now`
// like every live window in badgePoll.ts, so a badge Credly hasn't surfaced yet isn't missed.
function backfillWatermark(
  createdAt: string | null | undefined,
  now: Date,
): Record<string, unknown> {
  const top = new Date(now.getTime() - WINDOW_LAG_MS)
  return {
    since: top.toISOString(),
    backfill: {
      floor: releaseFloor(createdAt, top),
      top: top.toISOString(),
      since: null,
      until: null,
      page: 1,
    },
  }
}

// Onboarding is attempted for every mapped decision, not only a changed one: it is a cheap no-op
// once the unit exists, and it self-heals a run that died between the decision and the unit.
export async function applyDecisions(
  qx: QueryExecutor,
  decisions: ResolvedTemplate[],
  missing: PlanEntry[],
  templates: TemplateToResolve[],
  stored: ICredlyStoredTemplateMapping[],
  log: Logger,
): Promise<ApplyResult> {
  const result: ApplyResult = {
    outcomes: { inserted: 0, updated: 0, unchanged: 0, locked: 0 },
    integrationsCreated: 0,
    deactivated: 0,
    unitsOnboarded: 0,
    unitsDecommissioned: 0,
  }
  const createdAtById = new Map(templates.map((template) => [template.id, template.createdAt]))
  const integrationBySegment = new Map<string, string>()
  const now = new Date()
  for (const decision of decisions) {
    const segmentId = decision.status === 'mapped' ? decision.segmentId : null
    let integrationId: string | null = null
    if (segmentId) {
      integrationId = integrationBySegment.get(segmentId) ?? null
      if (!integrationId) {
        const integration = await findOrCreateCredlyIntegration(qx, segmentId)
        if (integration.created) result.integrationsCreated += 1
        integrationId = integration.id
        integrationBySegment.set(segmentId, integrationId)
      }
    }
    // One transaction per decision: a crash between the decision write, the unit's creation and
    // its lock must not leave the mapping pointing at an integration whose unit never landed.
    await qx.tx(async (txQx) => {
      const outcome = await upsertCredlyTemplateDecision(txQx, { ...decision, integrationId })
      result.outcomes[outcome] += 1
      if (outcome === 'locked') {
        log.warn(
          { badgeTemplateId: decision.badgeTemplateId, wanted: targetLabel(decision) },
          'credly template is locked, the new decision needs a review',
        )
        return
      }
      if (!segmentId || !integrationId) return
      const onboarded = await onboardCredlyTemplate(
        txQx,
        integrationId,
        decision.badgeTemplateId,
        decision.badgeTemplateName,
        backfillWatermark(createdAtById.get(decision.badgeTemplateId), now),
      )
      if (onboarded.outcome === 'created') {
        result.unitsOnboarded += 1
      } else if (onboarded.reason !== 'already_present') {
        log.warn(
          { badgeTemplateId: decision.badgeTemplateId, ...onboarded },
          'credly template onboarding skipped',
        )
      }
      // A unit synced under integrationId, so the mapping is frozen on it: a later resolve that
      // wants to move the template elsewhere gets 'locked' instead of silently orphaning the sync.
      if (onboarded.outcome === 'created' || onboarded.reason === 'already_present') {
        await lockCredlyTemplateMapping(txQx, decision.badgeTemplateId, {
          segmentId,
          integrationId,
        })
      }
    })
  }
  const integrationByTemplate = new Map(
    stored.map((row) => [row.badgeTemplateId, row.integrationId]),
  )
  const missingChannelIdsByIntegration = new Map<string, string[]>()
  const missingWithoutIntegration: string[] = []
  for (const entry of missing) {
    const integrationId = integrationByTemplate.get(entry.badgeTemplateId)
    if (integrationId) {
      missingChannelIdsByIntegration.set(integrationId, [
        ...(missingChannelIdsByIntegration.get(integrationId) ?? []),
        entry.badgeTemplateId,
      ])
    } else {
      missingWithoutIntegration.push(entry.badgeTemplateId)
    }
  }
  for (const badgeTemplateId of missingWithoutIntegration) {
    result.deactivated += await deactivateCredlyTemplateMapping(qx, badgeTemplateId)
  }
  // Deactivation and decommissioning happen in one transaction: a crash in between must not leave
  // a mapping deactivated with its sync unit still polling, since a deactivated row never comes
  // back as 'missing' on a later run, so the unit would then never be decommissioned.
  for (const [integrationId, channelIds] of missingChannelIdsByIntegration) {
    await qx.tx(async (txQx) => {
      for (const channelId of channelIds) {
        result.deactivated += await deactivateCredlyTemplateMapping(txQx, channelId)
      }
      result.unitsDecommissioned += await decommissionSyncUnitsForChannelIds(
        txQx,
        integrationId,
        channelIds,
      )
    })
  }
  return result
}
