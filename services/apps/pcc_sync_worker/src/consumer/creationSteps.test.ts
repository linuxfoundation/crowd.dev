import { describe, expect, test } from 'vitest'

import { SegmentType } from '@crowd/types'

import type { CdpCreationTarget, PccHierarchyNode } from '../parser'
import { buildCdpSegmentIndex, planCreation } from './creationPlan'
import type { CdpSegment } from './creationPlan'
import { buildCreationSteps, creationStepKey } from './creationSteps'
import type { CreationStep } from './creationSteps'

function node(slug: string, name = slug): PccHierarchyNode {
  return { pccProjectId: `pcc-${slug}`, name, slug }
}

function segment(
  type: SegmentType,
  linkedTo: PccHierarchyNode,
  overrides: Partial<CdpSegment> = {},
): CdpSegment {
  return {
    id: `${type}-${linkedTo.slug}`,
    type,
    slug: linkedTo.slug,
    name: linkedTo.name,
    sourceId: linkedTo.pccProjectId,
    parentId: null,
    ...overrides,
  }
}

function stepsFor(target: CdpCreationTarget, cdpSegments: CdpSegment[]): CreationStep[] {
  const index = buildCdpSegmentIndex(cdpSegments)
  const plan = planCreation(target, index)
  if (plan.action !== 'CREATE') throw new Error(`expected a CREATE plan, got ${plan.reason}`)
  return buildCreationSteps(target, plan, index)
}

const CNCF = node('cncf-pcc', 'Cloud Native Computing Foundation (CNCF)')
const KUBERNETES = node('kubernetes', 'Kubernetes')
const KUBECTL = node('kubectl', 'kubectl')
const KUSTOMIZE = node('kustomize', 'Kustomize')
const ZEPHYR = node('zephyr', 'Zephyr Project')

const cncfGroup = segment(SegmentType.PROJECT_GROUP, CNCF, { slug: 'cncf' })
const kubernetesProject = segment(SegmentType.PROJECT, KUBERNETES, {
  slug: 'k8s',
  parentId: cncfGroup.id,
})
const kubernetesSubproject = segment(SegmentType.SUB_PROJECT, KUBERNETES, {
  slug: 'k8s',
  parentId: kubernetesProject.id,
})

describe('buildCreationSteps', () => {
  test('creates a standalone project with one project group call', () => {
    const steps = stepsFor({ group: ZEPHYR, project: ZEPHYR, subprojects: [ZEPHYR] }, [])

    expect(steps).toEqual([{ endpoint: 'projectGroup', node: ZEPHYR }])
  })

  test('creates a project under an existing group with one project call', () => {
    const steps = stepsFor({ group: CNCF, project: KUBERNETES, subprojects: [KUBERNETES] }, [
      cncfGroup,
    ])

    expect(steps).toEqual([{ endpoint: 'project', node: KUBERNETES, parentSlug: 'cncf' }])
  })

  test('creates the group first, then the project under the slug of the new group', () => {
    const steps = stepsFor({ group: CNCF, project: KUBERNETES, subprojects: [KUBERNETES] }, [])

    expect(steps).toEqual([
      { endpoint: 'projectGroup', node: CNCF },
      { endpoint: 'project', node: KUBERNETES, parentSlug: CNCF.slug },
    ])
  })

  test('creates the levels below the project as sub-projects, top down', () => {
    const steps = stepsFor(
      { group: CNCF, project: KUBERNETES, subprojects: [KUBERNETES, KUBECTL, KUSTOMIZE] },
      [cncfGroup],
    )

    expect(steps).toEqual([
      { endpoint: 'project', node: KUBERNETES, parentSlug: 'cncf' },
      {
        endpoint: 'subproject',
        node: KUBECTL,
        parentSlug: KUBERNETES.slug,
        grandparentSlug: 'cncf',
      },
      {
        endpoint: 'subproject',
        node: KUSTOMIZE,
        parentSlug: KUBERNETES.slug,
        grandparentSlug: 'cncf',
      },
    ])
  })

  test('uses the slugs CDP has for the parents that already exist', () => {
    const steps = stepsFor(
      { group: CNCF, project: KUBERNETES, subprojects: [KUBERNETES, KUBECTL] },
      [cncfGroup, kubernetesProject, kubernetesSubproject],
    )

    expect(steps).toEqual([
      { endpoint: 'subproject', node: KUBECTL, parentSlug: 'k8s', grandparentSlug: 'cncf' },
    ])
  })

  test('creates the sub-project a project is missing for itself', () => {
    const steps = stepsFor({ group: CNCF, project: KUBERNETES, subprojects: [KUBERNETES] }, [
      cncfGroup,
      kubernetesProject,
    ])

    expect(steps).toEqual([
      { endpoint: 'subproject', node: KUBERNETES, parentSlug: 'k8s', grandparentSlug: 'cncf' },
    ])
  })

  test('needs nothing when every level exists', () => {
    const steps = stepsFor({ group: CNCF, project: KUBERNETES, subprojects: [KUBERNETES] }, [
      cncfGroup,
      kubernetesProject,
      kubernetesSubproject,
    ])

    expect(steps).toEqual([])
  })
})

describe('creationStepKey', () => {
  test('is the same for two projects that need the same new group', () => {
    const [groupForKubernetes] = stepsFor(
      { group: CNCF, project: KUBERNETES, subprojects: [KUBERNETES] },
      [],
    )
    const [groupForKubectl] = stepsFor(
      { group: CNCF, project: KUBECTL, subprojects: [KUBECTL] },
      [],
    )

    expect(creationStepKey(groupForKubernetes)).toBe(creationStepKey(groupForKubectl))
  })

  test('tells apart the same PCC project created at two levels', () => {
    const asGroup: CreationStep = { endpoint: 'projectGroup', node: ZEPHYR }
    const asProject: CreationStep = { endpoint: 'project', node: ZEPHYR, parentSlug: 'tlf' }

    expect(creationStepKey(asGroup)).not.toBe(creationStepKey(asProject))
  })
})
