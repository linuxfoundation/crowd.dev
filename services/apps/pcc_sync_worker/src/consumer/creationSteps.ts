import { SegmentType } from '@crowd/types'

import type { CdpCreationTarget, PccHierarchyNode } from '../parser'
import { findLinkedSegments, typedKey } from './creationPlan'
import type { CdpSegmentIndex, CreationPlan } from './creationPlan'

export type CreationEndpoint = 'projectGroup' | 'project' | 'subproject'

export interface CreationStep {
  endpoint: CreationEndpoint
  node: PccHierarchyNode
  parentSlug?: string
  grandparentSlug?: string
}

// The backend creates the levels below with the same name: a project group comes with its
// project and sub-project, a project with its sub-project.
const LEVELS_CREATED_BY: Record<CreationEndpoint, SegmentType[]> = {
  projectGroup: [SegmentType.PROJECT_GROUP, SegmentType.PROJECT, SegmentType.SUB_PROJECT],
  project: [SegmentType.PROJECT, SegmentType.SUB_PROJECT],
  subproject: [SegmentType.SUB_PROJECT],
}

export function creationStepKey(step: CreationStep): string {
  return `${step.endpoint}:${step.node.pccProjectId}`
}

function slugInCdp(index: CdpSegmentIndex, type: SegmentType, node: PccHierarchyNode): string {
  const [linked] = findLinkedSegments(index, type, node.pccProjectId)
  return linked?.slug ?? node.slug
}

export function buildCreationSteps(
  target: CdpCreationTarget,
  plan: Extract<CreationPlan, { action: 'CREATE' }>,
  index: CdpSegmentIndex,
): CreationStep[] {
  const missing = new Set(plan.segments.map((s) => typedKey(s.type, s.pccProjectId)))
  const steps: CreationStep[] = []

  const add = (step: CreationStep) => {
    steps.push(step)
    for (const type of LEVELS_CREATED_BY[step.endpoint]) {
      missing.delete(typedKey(type, step.node.pccProjectId))
    }
  }
  const needs = (type: SegmentType, node: PccHierarchyNode) =>
    missing.has(typedKey(type, node.pccProjectId))

  const groupSlug = slugInCdp(index, SegmentType.PROJECT_GROUP, target.group)
  const projectSlug = slugInCdp(index, SegmentType.PROJECT, target.project)

  if (needs(SegmentType.PROJECT_GROUP, target.group)) {
    add({ endpoint: 'projectGroup', node: target.group })
  }
  if (needs(SegmentType.PROJECT, target.project)) {
    add({ endpoint: 'project', node: target.project, parentSlug: groupSlug })
  }
  for (const node of target.subprojects) {
    if (needs(SegmentType.SUB_PROJECT, node)) {
      add({ endpoint: 'subproject', node, parentSlug: projectSlug, grandparentSlug: groupSlug })
    }
  }

  return steps
}
