import { DEFAULT_TENANT_ID } from '@crowd/common'
import type { QueryExecutor } from '@crowd/database'
import { SegmentType } from '@crowd/types'
import type { SegmentDbRow } from '@crowd/types'

import type { CdpCreationTarget, PccHierarchyNode } from '../parser'
import { addToIndex, nameKey, slugKey } from './existingProjects'

export type CdpSegment = Pick<
  SegmentDbRow,
  'id' | 'type' | 'slug' | 'name' | 'sourceId' | 'parentId'
>

export interface PlannedSegment extends PccHierarchyNode {
  type: SegmentType
}

export type CreationBlockReason =
  | 'INCOMPLETE_PCC_DATA'
  | 'AMBIGUOUS_PCC_ID'
  | 'EXISTS_UNDER_ANOTHER_PARENT'
  | 'EXISTS_WITHOUT_ITS_PARENT'
  | 'LINKED_AT_ANOTHER_LEVEL'
  | 'SLUG_OR_NAME_TAKEN'

export type CreationPlan =
  | { action: 'CREATE'; segments: PlannedSegment[] }
  | { action: 'BLOCKED'; reason: CreationBlockReason; details: Record<string, unknown> }

export interface CdpSegmentIndex {
  bySourceId: Map<string, CdpSegment[]>
  byAnyLevelSourceId: Map<string, CdpSegment[]>
  bySlug: Map<string, CdpSegment[]>
  byName: Map<string, CdpSegment[]>
}

type Parent = { id: string } | 'missing' | 'none'

type Resolution =
  | { state: 'exists'; segment: CdpSegment }
  | { state: 'missing' }
  | { state: 'blocked'; plan: CreationPlan }

// The backend creates a project group together with a project and a sub-project of the same name.
const LEVELS_CREATED_WITH_A_GROUP = [SegmentType.PROJECT, SegmentType.SUB_PROJECT]

export function typedKey(type: SegmentType, key: string): string {
  return `${type}:${key}`
}

export function buildCdpSegmentIndex(segments: CdpSegment[]): CdpSegmentIndex {
  const index: CdpSegmentIndex = {
    bySourceId: new Map(),
    byAnyLevelSourceId: new Map(),
    bySlug: new Map(),
    byName: new Map(),
  }
  for (const segment of segments) {
    if (segment.sourceId) {
      addToIndex(index.bySourceId, typedKey(segment.type, segment.sourceId), segment)
      addToIndex(index.byAnyLevelSourceId, segment.sourceId, segment)
    }
    addToIndex(index.bySlug, typedKey(segment.type, slugKey(segment.slug)), segment)
    if (segment.name) {
      addToIndex(index.byName, typedKey(segment.type, nameKey(segment.name)), segment)
    }
  }
  return index
}

export function findLinkedSegments(
  index: CdpSegmentIndex,
  type: SegmentType,
  pccProjectId: string,
): CdpSegment[] {
  return index.bySourceId.get(typedKey(type, pccProjectId)) ?? []
}

function blocked(reason: CreationBlockReason, details: Record<string, unknown>): Resolution {
  return { state: 'blocked', plan: { action: 'BLOCKED', reason, details } }
}

function describe(segments: CdpSegment[]): Omit<CdpSegment, 'sourceId'>[] {
  return segments.map(({ id, type, slug, name, parentId }) => ({ id, type, slug, name, parentId }))
}

function resolve(
  index: CdpSegmentIndex,
  type: SegmentType,
  node: PccHierarchyNode,
  parent: Parent,
  levelsOfNode: Set<SegmentType>,
): Resolution {
  const linked = findLinkedSegments(index, type, node.pccProjectId)

  if (linked.length > 1) {
    return blocked('AMBIGUOUS_PCC_ID', { type, node, segments: describe(linked) })
  }

  if (linked.length === 1) {
    const [segment] = linked
    if (parent === 'missing') {
      return blocked('EXISTS_WITHOUT_ITS_PARENT', { type, node, segments: describe(linked) })
    }
    if (parent !== 'none' && segment.parentId !== parent.id) {
      return blocked('EXISTS_UNDER_ANOTHER_PARENT', {
        type,
        node,
        expectedParentId: parent.id,
        segments: describe(linked),
      })
    }
    return { state: 'exists', segment }
  }

  const linkedAtAnotherLevel = (index.byAnyLevelSourceId.get(node.pccProjectId) ?? []).filter(
    (segment) => !levelsOfNode.has(segment.type),
  )
  if (linkedAtAnotherLevel.length > 0) {
    return blocked('LINKED_AT_ANOTHER_LEVEL', {
      type,
      node,
      segments: describe(linkedAtAnotherLevel),
    })
  }

  const taken = [
    ...(index.bySlug.get(typedKey(type, slugKey(node.slug))) ?? []),
    ...(index.byName.get(typedKey(type, nameKey(node.name))) ?? []),
  ]
  if (taken.length > 0) {
    const distinct = [...new Map(taken.map((segment) => [segment.id, segment])).values()]
    return blocked('SLUG_OR_NAME_TAKEN', { type, node, segments: describe(distinct) })
  }

  return { state: 'missing' }
}

function parentFrom(resolution: Resolution): Parent {
  return resolution.state === 'exists' ? { id: resolution.segment.id } : 'missing'
}

function levelsInTarget(target: CdpCreationTarget, pccProjectId: string): Set<SegmentType> {
  const levels = new Set<SegmentType>()
  if (target.group.pccProjectId === pccProjectId) levels.add(SegmentType.PROJECT_GROUP)
  if (target.project.pccProjectId === pccProjectId) levels.add(SegmentType.PROJECT)
  if (target.subprojects.some((node) => node.pccProjectId === pccProjectId)) {
    levels.add(SegmentType.SUB_PROJECT)
  }
  return levels
}

export function planCreation(
  target: CdpCreationTarget | null,
  index: CdpSegmentIndex,
): CreationPlan {
  if (!target) return { action: 'BLOCKED', reason: 'INCOMPLETE_PCC_DATA', details: {} }

  const segments: PlannedSegment[] = []
  const levelsOf = (node: PccHierarchyNode) => levelsInTarget(target, node.pccProjectId)

  const groupLevels = levelsOf(target.group)
  const group = resolve(index, SegmentType.PROJECT_GROUP, target.group, 'none', groupLevels)
  if (group.state === 'blocked') return group.plan
  if (group.state === 'missing') {
    segments.push({ ...target.group, type: SegmentType.PROJECT_GROUP })

    if (target.project.pccProjectId !== target.group.pccProjectId) {
      for (const type of LEVELS_CREATED_WITH_A_GROUP) {
        const counterpart = resolve(index, type, target.group, 'missing', groupLevels)
        if (counterpart.state === 'blocked') return counterpart.plan
        segments.push({ ...target.group, type })
      }
    }
  }

  const project = resolve(
    index,
    SegmentType.PROJECT,
    target.project,
    parentFrom(group),
    levelsOf(target.project),
  )
  if (project.state === 'blocked') return project.plan
  if (project.state === 'missing') segments.push({ ...target.project, type: SegmentType.PROJECT })

  for (const node of target.subprojects) {
    const subproject = resolve(
      index,
      SegmentType.SUB_PROJECT,
      node,
      parentFrom(project),
      levelsOf(node),
    )
    if (subproject.state === 'blocked') return subproject.plan
    if (subproject.state === 'missing') segments.push({ ...node, type: SegmentType.SUB_PROJECT })
  }

  return { action: 'CREATE', segments }
}

export async function loadCdpSegmentIndex(qx: QueryExecutor): Promise<CdpSegmentIndex> {
  const segments: CdpSegment[] = await qx.select(
    `
    SELECT id, type, slug, name, "sourceId", "parentId"
    FROM segments
    WHERE "tenantId" = $(tenantId)
    `,
    { tenantId: DEFAULT_TENANT_ID },
  )
  return buildCdpSegmentIndex(segments)
}

export interface CreationSummary {
  creatableProjects: number
  blockedByReason: Map<CreationBlockReason, number>
  segmentsToCreate: Map<SegmentType, Set<string>>
}

export function emptyCreationSummary(): CreationSummary {
  return { creatableProjects: 0, blockedByReason: new Map(), segmentsToCreate: new Map() }
}

export function addToCreationSummary(summary: CreationSummary, plan: CreationPlan): void {
  if (plan.action === 'BLOCKED') {
    summary.blockedByReason.set(plan.reason, (summary.blockedByReason.get(plan.reason) ?? 0) + 1)
    return
  }
  summary.creatableProjects++
  for (const segment of plan.segments) {
    const pccProjectIds = summary.segmentsToCreate.get(segment.type) ?? new Set<string>()
    pccProjectIds.add(segment.pccProjectId)
    summary.segmentsToCreate.set(segment.type, pccProjectIds)
  }
}

export function creationSummaryCounts(summary: CreationSummary): Record<string, unknown> {
  return {
    creatableProjects: summary.creatableProjects,
    blockedProjects: Object.fromEntries(summary.blockedByReason),
    segmentsToCreate: Object.fromEntries(
      [...summary.segmentsToCreate].map(([type, pccProjectIds]) => [type, pccProjectIds.size]),
    ),
  }
}
