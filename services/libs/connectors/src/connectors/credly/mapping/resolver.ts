import type { CredlyHoldReason, ICredlyTemplateDecision } from '@crowd/data-access-layer/src/credly'

import type { Catalogue, CatalogueCourse, CatalogueIndex } from './catalogue'
import { indexCatalogue, normalizeTitle } from './catalogue'
import { extractCourseCode, normalizeSku, skuBase } from './courseCode'
import type { CredlyTemplateOverride } from './overrides'

export const RULE_VERSION = 1

export type CredlyMappingRule =
  | 'exact_sku'
  | 'sku_vote'
  | 'base_sku'
  | 'title'
  | 'override'
  | 'hold'
  | 'excluded'

export interface TemplateToResolve {
  id: string
  name: string
  badgeCount?: number | null
}

export interface ResolvedTemplate extends Required<
  Omit<ICredlyTemplateDecision, 'integrationId' | 'rule' | 'evidence'>
> {
  rule: CredlyMappingRule
  evidence: Record<string, unknown>
}

export interface ResolverInput {
  templates: TemplateToResolve[]
  catalogue: Catalogue
  segmentIdBySlug: Map<string, string>
  overrides: readonly CredlyTemplateOverride[]
}

interface ProjectMatch {
  projectSlug: string
  rule: CredlyMappingRule
  matchedSku: string | null
  evidence: Record<string, unknown>
}

interface Hold {
  holdReason: CredlyHoldReason
  evidence: Record<string, unknown>
}

type Outcome = ProjectMatch | Hold | 'excluded'

const TEST_BADGE_NAME = /badge test|test badge|do_not_use|^test\b/i

function uniqueProjects(slugs: (string | undefined)[]): string[] {
  return [...new Set(slugs.filter((slug): slug is string => !!slug))]
}

function liveFirst(courses: CatalogueCourse[]): CatalogueCourse[] {
  const live = courses.filter((course) => !course.isTestOrArchived)
  return live.length > 0 ? live : courses
}

function groupProjects(index: CatalogueIndex, courses: CatalogueCourse[]): string[] {
  return uniqueProjects(
    courses.map((course) =>
      course.courseGroupId ? index.projectByGroup.get(course.courseGroupId) : undefined,
    ),
  )
}

function exactSku(index: CatalogueIndex, code: string): ProjectMatch | Hold | null {
  const courses = index.coursesBySku.get(code)
  if (!courses) return null
  const projects = groupProjects(index, liveFirst(courses))
  if (projects.length === 1) {
    return { projectSlug: projects[0], rule: 'exact_sku', matchedSku: code, evidence: { code } }
  }
  if (projects.length > 1) {
    return { holdReason: 'ambiguous', evidence: { code, projects } }
  }
  return null
}

function skuVote(index: CatalogueIndex, code: string): ProjectMatch | Hold | null {
  const rows = index.enrollmentsByCode.get(code)
  if (!rows?.length) return null
  const perProject = new Map<string, number>()
  for (const row of rows) {
    perProject.set(row.projectSlug, (perProject.get(row.projectSlug) ?? 0) + row.enrollments)
  }
  const votes = [...perProject]
    .map(([projectSlug, enrollments]) => ({ projectSlug, enrollments }))
    .sort((a, b) => b.enrollments - a.enrollments)
  if (votes.length > 1 && votes[0].enrollments === votes[1].enrollments) {
    return { holdReason: 'ambiguous', evidence: { code, votes } }
  }
  return {
    projectSlug: votes[0].projectSlug,
    rule: 'sku_vote',
    matchedSku: code,
    evidence: { code, votes },
  }
}

function baseSku(index: CatalogueIndex, code: string): ProjectMatch | Hold | null {
  const base = skuBase(code)
  const variants = index.coursesByBase.get(base)
  if (!variants) return null
  const projects = uniqueProjects([
    ...groupProjects(index, variants),
    ...variants.flatMap((course) =>
      (index.enrollmentsByCode.get(normalizeSku(course.sku)) ?? []).map((e) => e.projectSlug),
    ),
  ])
  if (projects.length === 1) {
    return {
      projectSlug: projects[0],
      rule: 'base_sku',
      matchedSku: base,
      evidence: { code, base, variants: variants.map((course) => course.sku) },
    }
  }
  if (projects.length > 1) {
    return { holdReason: 'ambiguous', evidence: { code, base, projects } }
  }
  return null
}

function titleMatch(index: CatalogueIndex, name: string): ProjectMatch | Hold | null {
  const title = normalizeTitle(name)
  const matches = title ? index.projectsByTitle.get(title) : undefined
  if (!matches) return null
  const projects = uniqueProjects(matches.map((match) => match.projectSlug))
  if (projects.length === 1) {
    return {
      projectSlug: projects[0],
      rule: 'title',
      matchedSku: null,
      evidence: { title, matches: matches.map((match) => match.source) },
    }
  }
  return { holdReason: 'ambiguous', evidence: { title, projects } }
}

function isHold(outcome: ProjectMatch | Hold): outcome is Hold {
  return 'holdReason' in outcome
}

function decide(index: CatalogueIndex, template: TemplateToResolve): ProjectMatch | Hold {
  if (TEST_BADGE_NAME.test(template.name)) {
    return { holdReason: 'test', evidence: { name: template.name } }
  }
  const code = extractCourseCode(template.name)
  if (!code) {
    return titleMatch(index, template.name) ?? { holdReason: 'no_match', evidence: {} }
  }
  const sku = normalizeSku(code.code)
  const outcome =
    exactSku(index, sku) ??
    skuVote(index, sku) ??
    baseSku(index, sku) ??
    titleMatch(index, template.name)
  if (outcome) return outcome
  const known = index.coursesBySku.has(sku) || index.coursesByBase.has(code.base)
  return { holdReason: known ? 'no_project' : 'no_match', evidence: { code: sku } }
}

function applyOverride(template: TemplateToResolve, override: CredlyTemplateOverride): Outcome {
  if (override.status === 'excluded') return 'excluded'
  return {
    projectSlug: override.projectSlug,
    rule: 'override',
    matchedSku: null,
    evidence: { note: override.note, template: template.name },
  }
}

export function resolveTemplates(input: ResolverInput): ResolvedTemplate[] {
  const index = indexCatalogue(input.catalogue)
  const overrides = new Map(input.overrides.map((o) => [o.badgeTemplateId, o]))
  const segmentIdBySlug = new Map(
    [...input.segmentIdBySlug].map(([slug, id]) => [slug.toLowerCase(), id]),
  )

  return input.templates.map((template) => {
    const override = overrides.get(template.id)
    const outcome = override ? applyOverride(template, override) : decide(index, template)
    const base = {
      badgeTemplateId: template.id,
      badgeTemplateName: template.name,
      badgeCount: template.badgeCount ?? null,
      ruleVersion: RULE_VERSION,
      reviewedBy: override?.reviewedBy ?? null,
      segmentId: null,
    }

    if (outcome === 'excluded') {
      return {
        ...base,
        status: 'excluded',
        holdReason: null,
        projectSlug: null,
        rule: 'excluded',
        matchedSku: null,
        evidence: { note: override?.note },
      }
    }
    if (isHold(outcome)) {
      return {
        ...base,
        status: 'held',
        holdReason: outcome.holdReason,
        projectSlug: null,
        rule: 'hold',
        matchedSku: null,
        evidence: outcome.evidence,
      }
    }
    const segmentId = segmentIdBySlug.get(outcome.projectSlug.toLowerCase()) ?? null
    return {
      ...base,
      status: segmentId ? 'mapped' : 'held',
      holdReason: segmentId ? null : 'no_segment',
      projectSlug: outcome.projectSlug,
      segmentId,
      rule: outcome.rule,
      matchedSku: outcome.matchedSku,
      evidence: outcome.evidence,
    }
  })
}
