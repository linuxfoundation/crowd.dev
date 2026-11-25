import { describe, expect, test } from 'vitest'

import { SegmentType } from '@crowd/types'

import type { CdpCreationTarget, PccHierarchyNode } from '../parser'
import {
  addToCreationSummary,
  buildCdpSegmentIndex,
  creationSummaryCounts,
  emptyCreationSummary,
  planCreation,
} from './creationPlan'
import type { CdpSegment } from './creationPlan'

function node(slug: string, name = slug): PccHierarchyNode {
  return { pccProjectId: `pcc-${slug}`, name, slug }
}

function segment(
  type: SegmentType,
  linkedTo: PccHierarchyNode | null,
  overrides: Partial<CdpSegment> = {},
): CdpSegment {
  const slug = linkedTo?.slug ?? overrides.slug ?? 'unlinked'
  return {
    id: `${type}-${slug}`,
    type,
    slug,
    name: linkedTo?.name ?? slug,
    sourceId: linkedTo?.pccProjectId ?? null,
    parentId: null,
    ...overrides,
  }
}

const LFN = node('lfn', 'LF Networking (LFN)')
const ANUKET = node('anuket', 'Anuket')
const RC1 = node('rc1', 'RC1')
const ANUKET_RC1: CdpCreationTarget = { group: LFN, project: ANUKET, subprojects: [ANUKET, RC1] }

const lfnGroup = segment(SegmentType.PROJECT_GROUP, LFN)
const anuketProject = segment(SegmentType.PROJECT, ANUKET, { parentId: lfnGroup.id })
const anuketSubproject = segment(SegmentType.SUB_PROJECT, ANUKET, { parentId: anuketProject.id })

describe('planCreation', () => {
  test('plans the project and sub-project the backend creates with a new group', () => {
    const plan = planCreation(ANUKET_RC1, buildCdpSegmentIndex([]))

    expect(plan).toEqual({
      action: 'CREATE',
      segments: [
        { ...LFN, type: SegmentType.PROJECT_GROUP },
        { ...LFN, type: SegmentType.PROJECT },
        { ...LFN, type: SegmentType.SUB_PROJECT },
        { ...ANUKET, type: SegmentType.PROJECT },
        { ...ANUKET, type: SegmentType.SUB_PROJECT },
        { ...RC1, type: SegmentType.SUB_PROJECT },
      ],
    })
  })

  test('creates only the project and its sub-projects under an existing group', () => {
    const plan = planCreation(ANUKET_RC1, buildCdpSegmentIndex([lfnGroup]))

    expect(plan).toEqual({
      action: 'CREATE',
      segments: [
        { ...ANUKET, type: SegmentType.PROJECT },
        { ...ANUKET, type: SegmentType.SUB_PROJECT },
        { ...RC1, type: SegmentType.SUB_PROJECT },
      ],
    })
  })

  test('creates only the leaf under an existing project', () => {
    const index = buildCdpSegmentIndex([lfnGroup, anuketProject, anuketSubproject])

    expect(planCreation(ANUKET_RC1, index)).toEqual({
      action: 'CREATE',
      segments: [{ ...RC1, type: SegmentType.SUB_PROJECT }],
    })
  })

  test('creates the sub-project a project is missing for itself', () => {
    const index = buildCdpSegmentIndex([lfnGroup, anuketProject])

    expect(planCreation(ANUKET_RC1, index)).toMatchObject({
      action: 'CREATE',
      segments: [
        { ...ANUKET, type: SegmentType.SUB_PROJECT },
        { ...RC1, type: SegmentType.SUB_PROJECT },
      ],
    })
  })

  test('uses the same PCC project at group, project and sub-project level', () => {
    const zephyr = node('zephyr')
    const target = { group: zephyr, project: zephyr, subprojects: [zephyr] }

    expect(planCreation(target, buildCdpSegmentIndex([]))).toMatchObject({
      action: 'CREATE',
      segments: [
        { type: SegmentType.PROJECT_GROUP },
        { type: SegmentType.PROJECT },
        { type: SegmentType.SUB_PROJECT },
      ],
    })
  })

  test('blocks a project the parser could not build a target for', () => {
    expect(planCreation(null, buildCdpSegmentIndex([]))).toEqual({
      action: 'BLOCKED',
      reason: 'INCOMPLETE_PCC_DATA',
      details: {},
    })
  })

  test('blocks when two CDP groups carry the PCC ID of the group', () => {
    const duplicate = { ...lfnGroup, id: 'group-lfn-copy', slug: 'lfn-copy' }

    expect(planCreation(ANUKET_RC1, buildCdpSegmentIndex([lfnGroup, duplicate]))).toMatchObject({
      action: 'BLOCKED',
      reason: 'AMBIGUOUS_PCC_ID',
      details: { type: SegmentType.PROJECT_GROUP, node: LFN },
    })
  })

  test('blocks when the project exists under another group', () => {
    const elsewhere = { ...anuketProject, parentId: 'another-group' }

    expect(planCreation(ANUKET_RC1, buildCdpSegmentIndex([lfnGroup, elsewhere]))).toMatchObject({
      action: 'BLOCKED',
      reason: 'EXISTS_UNDER_ANOTHER_PARENT',
      details: { type: SegmentType.PROJECT, node: ANUKET, expectedParentId: lfnGroup.id },
    })
  })

  test('blocks when the project exists but its group does not', () => {
    expect(planCreation(ANUKET_RC1, buildCdpSegmentIndex([anuketProject]))).toMatchObject({
      action: 'BLOCKED',
      reason: 'EXISTS_WITHOUT_ITS_PARENT',
      details: { type: SegmentType.PROJECT },
    })
  })

  test('blocks when CDP has the PCC group linked as a project', () => {
    const groupAsProject = segment(SegmentType.PROJECT, null, {
      slug: 'networking',
      name: 'Networking',
      sourceId: LFN.pccProjectId,
    })

    expect(planCreation(ANUKET_RC1, buildCdpSegmentIndex([groupAsProject]))).toMatchObject({
      action: 'BLOCKED',
      reason: 'LINKED_AT_ANOTHER_LEVEL',
      details: { type: SegmentType.PROJECT_GROUP, segments: [{ id: groupAsProject.id }] },
    })
  })

  test('blocks when a group with the same slug is not linked to PCC', () => {
    const unlinked = segment(SegmentType.PROJECT_GROUP, null, { slug: 'LFN', name: 'Networking' })

    expect(planCreation(ANUKET_RC1, buildCdpSegmentIndex([unlinked]))).toMatchObject({
      action: 'BLOCKED',
      reason: 'SLUG_OR_NAME_TAKEN',
      details: { type: SegmentType.PROJECT_GROUP, segments: [{ id: unlinked.id }] },
    })
  })

  test('blocks when a non-LF project has the slug behind its prefix', () => {
    const nonLf = segment(SegmentType.PROJECT, null, { slug: 'nonlf_anuket', name: 'anuket app' })

    expect(planCreation(ANUKET_RC1, buildCdpSegmentIndex([lfnGroup, nonLf]))).toMatchObject({
      action: 'BLOCKED',
      reason: 'SLUG_OR_NAME_TAKEN',
      details: { type: SegmentType.PROJECT },
    })
  })

  test('blocks when a sub-project of another project has the same name', () => {
    const sameName = segment(SegmentType.SUB_PROJECT, null, { slug: 'release-1', name: ' rc1 ' })
    const index = buildCdpSegmentIndex([lfnGroup, anuketProject, anuketSubproject, sameName])

    expect(planCreation(ANUKET_RC1, index)).toMatchObject({
      action: 'BLOCKED',
      reason: 'SLUG_OR_NAME_TAKEN',
      details: { type: SegmentType.SUB_PROJECT, node: RC1 },
    })
  })

  test('blocks a new group when a project has its slug', () => {
    const projectNamedLikeGroup = segment(SegmentType.PROJECT, null, { slug: 'lfn', name: 'LFN' })

    expect(planCreation(ANUKET_RC1, buildCdpSegmentIndex([projectNamedLikeGroup]))).toMatchObject({
      action: 'BLOCKED',
      reason: 'SLUG_OR_NAME_TAKEN',
      details: {
        type: SegmentType.PROJECT,
        node: LFN,
        segments: [{ id: projectNamedLikeGroup.id }],
      },
    })
  })

  test('blocks a new group when a sub-project has its name', () => {
    const subprojectNamedLikeGroup = segment(SegmentType.SUB_PROJECT, null, {
      slug: 'networking',
      name: LFN.name,
    })
    const index = buildCdpSegmentIndex([subprojectNamedLikeGroup])

    expect(planCreation(ANUKET_RC1, index)).toMatchObject({
      action: 'BLOCKED',
      reason: 'SLUG_OR_NAME_TAKEN',
      details: { type: SegmentType.SUB_PROJECT, node: LFN },
    })
  })

  test('ignores a project with the slug of a group that already exists', () => {
    const projectNamedLikeGroup = segment(SegmentType.PROJECT, null, { slug: 'lfn', name: 'LFN' })
    const index = buildCdpSegmentIndex([lfnGroup, projectNamedLikeGroup])

    expect(planCreation(ANUKET_RC1, index)).toMatchObject({ action: 'CREATE' })
  })
})

describe('creation summary', () => {
  test('counts a segment once when several projects need it', () => {
    const summary = emptyCreationSummary()
    const rc2: CdpCreationTarget = { ...ANUKET_RC1, subprojects: [ANUKET, node('rc2')] }

    addToCreationSummary(summary, planCreation(ANUKET_RC1, buildCdpSegmentIndex([])))
    addToCreationSummary(summary, planCreation(rc2, buildCdpSegmentIndex([])))
    addToCreationSummary(summary, planCreation(null, buildCdpSegmentIndex([])))

    expect(creationSummaryCounts(summary)).toEqual({
      creatableProjects: 2,
      blockedProjects: { INCOMPLETE_PCC_DATA: 1 },
      segmentsToCreate: { projectGroup: 1, project: 2, subproject: 4 },
    })
  })
})
