import { describe, expect, test, vi } from 'vitest'

import { SegmentStatus } from '@crowd/types'

import { runCreations } from './creationRun'
import type { ProjectToCreate } from './creationRun'
import type { CreationStep } from './creationSteps'
import type { SegmentCreationResult } from './segmentCreationClient'

const CNCF = { pccProjectId: 'pcc-cncf', name: 'CNCF', slug: 'cncf' }

const cncfGroupStep: CreationStep = { endpoint: 'projectGroup', node: CNCF }

function projectUnderCncf(slug: string, status: SegmentStatus | null = null): ProjectToCreate {
  const node = { pccProjectId: `pcc-${slug}`, name: slug, slug }
  return {
    pccProjectId: node.pccProjectId,
    pccSlug: slug,
    name: slug,
    status,
    steps: [
      cncfGroupStep,
      { endpoint: 'project', node, parent: { slug: CNCF.slug, name: CNCF.name } },
    ],
  }
}

function standaloneProject(slug: string): ProjectToCreate {
  const node = { pccProjectId: `pcc-${slug}`, name: slug, slug }
  return {
    pccProjectId: node.pccProjectId,
    pccSlug: slug,
    name: slug,
    status: null,
    steps: [{ endpoint: 'projectGroup', node }],
  }
}

function projectWithItsOwnGroup(slug: string): ProjectToCreate {
  const group = { pccProjectId: `pcc-${slug}-group`, name: `${slug} group`, slug: `${slug}-group` }
  const node = { pccProjectId: `pcc-${slug}`, name: slug, slug }
  return {
    pccProjectId: node.pccProjectId,
    pccSlug: slug,
    name: slug,
    status: null,
    steps: [
      { endpoint: 'projectGroup', node: group },
      { endpoint: 'project', node, parent: { slug: group.slug, name: group.name } },
    ],
  }
}

const UNANSWERED: SegmentCreationResult = {
  outcome: 'failed',
  httpStatus: null,
  error: 'fetch failed',
}
const CREATED: SegmentCreationResult = { outcome: 'created' }
const REFUSED: SegmentCreationResult = { outcome: 'failed', httpStatus: 400, error: 'HTTP 400' }

function backendAnswering(answer: (step: CreationStep) => SegmentCreationResult = () => CREATED) {
  return vi.fn(async (step: CreationStep, _status: SegmentStatus | null) => answer(step))
}

function calledEndpoints(createStep: ReturnType<typeof backendAnswering>): string[] {
  return createStep.mock.calls.map(([step]) => `${step.endpoint}:${step.node.slug}`)
}

describe('runCreations', () => {
  test('creates a level shared by two projects only once', async () => {
    const createStep = backendAnswering()

    const result = await runCreations([projectUnderCncf('k8s'), projectUnderCncf('helm')], {
      maxProjects: 10,
      createStep,
    })

    expect(calledEndpoints(createStep)).toEqual([
      'projectGroup:cncf',
      'project:k8s',
      'project:helm',
    ])
    expect(result.outcomes.map((o) => o.outcome)).toEqual(['created', 'created'])
    expect(result.notAttempted).toBe(0)
  })

  test('sends the pcc status only with the level of the project itself', async () => {
    const createStep = backendAnswering()

    await runCreations([projectUnderCncf('k8s', SegmentStatus.ARCHIVED)], {
      maxProjects: 10,
      createStep,
    })

    expect(createStep.mock.calls.map(([, status]) => status)).toEqual([
      null,
      SegmentStatus.ARCHIVED,
    ])
  })

  test('sends the status of a shared level even when another project creates it first', async () => {
    const createStep = backendAnswering()
    const cncf: ProjectToCreate = {
      pccProjectId: CNCF.pccProjectId,
      pccSlug: CNCF.slug,
      name: CNCF.name,
      status: SegmentStatus.ARCHIVED,
      steps: [cncfGroupStep],
    }

    const result = await runCreations([projectUnderCncf('k8s'), cncf], {
      maxProjects: 10,
      createStep,
    })

    expect(createStep.mock.calls.map(([, status]) => status)).toEqual([
      SegmentStatus.ARCHIVED,
      null,
    ])
    expect(result.outcomes.map((o) => o.outcome)).toEqual(['created', 'created'])
  })

  test('stops at the cap and counts the projects left for the next run', async () => {
    const createStep = backendAnswering()

    const result = await runCreations(
      [standaloneProject('a'), standaloneProject('b'), standaloneProject('c')],
      { maxProjects: 2, createStep },
    )

    expect(calledEndpoints(createStep)).toEqual(['projectGroup:a', 'projectGroup:b'])
    expect(result.notAttempted).toBe(1)
    expect(result.stoppedWithoutAnswers).toBe(false)
  })

  test('skips the remaining levels of a project when one is refused', async () => {
    const createStep = backendAnswering((step) => (step.node.slug === 'cncf' ? REFUSED : CREATED))

    const result = await runCreations([projectUnderCncf('k8s')], { maxProjects: 10, createStep })

    expect(calledEndpoints(createStep)).toEqual(['projectGroup:cncf'])
    expect(result.outcomes).toEqual([
      expect.objectContaining({ outcome: 'failed', step: cncfGroupStep, httpStatus: 400 }),
    ])
  })

  test('does not retry in the same run a level that already failed', async () => {
    const createStep = backendAnswering((step) => (step.node.slug === 'cncf' ? REFUSED : CREATED))

    const result = await runCreations(
      [projectUnderCncf('k8s'), projectUnderCncf('helm'), standaloneProject('zephyr')],
      { maxProjects: 2, createStep },
    )

    expect(calledEndpoints(createStep)).toEqual(['projectGroup:cncf', 'projectGroup:zephyr'])
    expect(result.outcomes.map((o) => o.outcome)).toEqual([
      'failed',
      'waiting_for_failed_level',
      'created',
    ])
  })

  test('stops the run when the backend does not answer several times in a row', async () => {
    const createStep = backendAnswering(() => ({
      outcome: 'failed',
      httpStatus: null,
      error: 'fetch failed',
    }))

    const result = await runCreations(
      [standaloneProject('a'), standaloneProject('b'), standaloneProject('c')],
      { maxProjects: 10, maxConsecutiveUnanswered: 2, createStep },
    )

    expect(createStep).toHaveBeenCalledTimes(2)
    expect(result.notAttempted).toBe(1)
    expect(result.stoppedWithoutAnswers).toBe(true)
  })

  test('keeps going when the backend refuses projects', async () => {
    const createStep = backendAnswering(() => REFUSED)

    const result = await runCreations(
      [standaloneProject('a'), standaloneProject('b'), standaloneProject('c')],
      { maxProjects: 10, maxConsecutiveUnanswered: 2, createStep },
    )

    expect(createStep).toHaveBeenCalledTimes(3)
    expect(result.stoppedWithoutAnswers).toBe(false)
  })

  test('does not stop when the backend answers between two requests left unanswered', async () => {
    const createStep = backendAnswering((step) =>
      step.endpoint === 'projectGroup' ? CREATED : UNANSWERED,
    )

    const result = await runCreations(
      ['a', 'b', 'c', 'd'].map((slug) => projectWithItsOwnGroup(slug)),
      { maxProjects: 10, maxConsecutiveUnanswered: 3, createStep },
    )

    expect(createStep).toHaveBeenCalledTimes(8)
    expect(result.notAttempted).toBe(0)
    expect(result.stoppedWithoutAnswers).toBe(false)
  })

  test('does not report a stop when the last project is the one left unanswered', async () => {
    const createStep = backendAnswering(() => UNANSWERED)

    const result = await runCreations([standaloneProject('a'), standaloneProject('b')], {
      maxProjects: 10,
      maxConsecutiveUnanswered: 2,
      createStep,
    })

    expect(result.notAttempted).toBe(0)
    expect(result.stoppedWithoutAnswers).toBe(false)
  })
})
