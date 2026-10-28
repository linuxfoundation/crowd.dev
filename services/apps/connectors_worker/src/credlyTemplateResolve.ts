import type { ConnectorHttp } from '@crowd/connectors'
import { fetchBadgeTemplatesPage } from '@crowd/connectors/src/connectors/credly/apiClient'
import type { Catalogue } from '@crowd/connectors/src/connectors/credly/mapping/catalogue'
import type {
  ResolvedTemplate,
  TemplateToResolve,
} from '@crowd/connectors/src/connectors/credly/mapping/resolver'
import type {
  CredlyDecisionOutcome,
  ICredlyStoredTemplateMapping,
} from '@crowd/data-access-layer/src/credly'
import {
  deactivateCredlyTemplateMapping,
  findOrCreateCredlyIntegration,
  upsertCredlyTemplateDecision,
} from '@crowd/data-access-layer/src/credly'
import type { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { listActiveSubprojectSegments } from '@crowd/data-access-layer/src/segments'
import type { Logger } from '@crowd/logging'

export interface CatalogueSource {
  run<T>(query: string): Promise<T[]>
}

export async function loadCatalogue(source: CatalogueSource): Promise<Catalogue> {
  const [courses, courseGroups, enrollments] = await Promise.all([
    source.run<Catalogue['courses'][number]>(
      `SELECT sku AS "sku", title AS "title", course_group_id AS "courseGroupId",
              COALESCE(is_test_or_archived, FALSE) AS "isTestOrArchived"
       FROM bronze_census_ti.courses
       WHERE sku IS NOT NULL AND TRIM(sku) <> ''`,
    ),
    source.run<Catalogue['courseGroups'][number]>(
      `SELECT course_group_id AS "courseGroupId", title AS "title", project_slug AS "projectSlug"
       FROM silver_dim._ti_course_groups_to_projects
       WHERE project_slug IS NOT NULL AND TRIM(project_slug) <> ''`,
    ),
    source.run<Catalogue['enrollments'][number]>(
      `SELECT course_code AS "courseCode", project_slug AS "projectSlug", COUNT(*) AS "enrollments"
       FROM silver_fact.enrollments
       WHERE course_code IS NOT NULL AND project_slug IS NOT NULL
       GROUP BY 1, 2`,
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
  let expected = 0
  for (let page = 1; ; page += 1) {
    const result = await fetchBadgeTemplatesPage(http, { organizationId, page }, log)
    if (page === 1) expected = result.metadata.total_count
    for (const template of result.data) {
      const count = template.badges_count
      templates.push({
        id: template.id,
        name: template.name,
        badgeCount: typeof count === 'number' && Number.isFinite(count) ? count : null,
      })
    }
    if (result.data.length === 0 || page >= result.metadata.total_pages) break
  }
  return { templates, complete: templates.length === expected }
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

const COMPARED_FIELDS = [
  'badgeTemplateName',
  'status',
  'holdReason',
  'projectSlug',
  'segmentId',
  'rule',
  'ruleVersion',
  'matchedSku',
] as const

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

function decisionChanged(stored: ICredlyStoredTemplateMapping, next: ResolvedTemplate): boolean {
  return (
    stored.deactivatedAt !== null ||
    COMPARED_FIELDS.some((field) => (stored[field] ?? null) !== (next[field] ?? null))
  )
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
}

export async function applyDecisions(
  qx: QueryExecutor,
  decisions: ResolvedTemplate[],
  missing: PlanEntry[],
  log: Logger,
): Promise<ApplyResult> {
  const result: ApplyResult = {
    outcomes: { inserted: 0, updated: 0, unchanged: 0, locked: 0 },
    integrationsCreated: 0,
    deactivated: 0,
  }
  const integrationBySegment = new Map<string, string>()
  for (const decision of decisions) {
    let integrationId: string | null = null
    if (decision.status === 'mapped' && decision.segmentId) {
      integrationId = integrationBySegment.get(decision.segmentId) ?? null
      if (!integrationId) {
        const integration = await findOrCreateCredlyIntegration(qx, decision.segmentId)
        if (integration.created) result.integrationsCreated += 1
        integrationId = integration.id
        integrationBySegment.set(decision.segmentId, integrationId)
      }
    }
    const outcome = await upsertCredlyTemplateDecision(qx, { ...decision, integrationId })
    result.outcomes[outcome] += 1
    if (outcome === 'locked') {
      log.warn(
        { badgeTemplateId: decision.badgeTemplateId, wanted: targetLabel(decision) },
        'credly template is locked, the new decision needs a review',
      )
    }
  }
  for (const entry of missing) {
    result.deactivated += await deactivateCredlyTemplateMapping(qx, entry.badgeTemplateId)
  }
  return result
}
