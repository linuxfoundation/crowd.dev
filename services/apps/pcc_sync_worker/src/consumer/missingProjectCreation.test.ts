import { describe, expect, test, vi } from 'vitest'

import { SegmentStatus, SegmentType } from '@crowd/types'

import type { ParsedPccProject } from '../parser'
import { buildCdpSegmentIndex, planCreation } from './creationPlan'
import type { ProjectToCreate } from './creationRun'
import type { CreationStep } from './creationSteps'
import {
  buildCreationFailureAlert,
  createMissingProjects,
  describeBlockedCreation,
  effectiveCreationMode,
  readCreationSettings,
  toProjectToCreate,
} from './missingProjectCreation'
import type { CreationFailure, MissingProjectCreationSummary } from './missingProjectCreation'
import type { SegmentCreationResult } from './segmentCreationClient'

const ZEPHYR = {
  pccProjectId: 'pcc-zephyr',
  name: 'Zephyr Project',
  slug: 'zephyr',
}

function zephyrInPcc(overrides: Partial<ParsedPccProject> = {}): ParsedPccProject {
  return {
    pccProjectId: ZEPHYR.pccProjectId,
    pccSlug: ZEPHYR.slug,
    name: ZEPHYR.name,
    status: SegmentStatus.ARCHIVED,
    maturity: null,
    description: null,
    logoUrl: null,
    segmentIdsFromSnowflake: [],
    cdpTarget: {
      group: ZEPHYR.name,
      project: ZEPHYR.name,
      subproject: ZEPHYR.name,
    },
    creationTarget: { group: ZEPHYR, project: ZEPHYR, subprojects: [ZEPHYR] },
    ...overrides,
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

function backendAnswering(answer: (step: CreationStep) => SegmentCreationResult) {
  return vi.fn(async (step: CreationStep, _status: SegmentStatus | null) => answer(step))
}

function failureRecorder() {
  return vi.fn(async (_failure: CreationFailure) => undefined)
}

describe('readCreationSettings', () => {
  test('is off with a cap of 30 when nothing is set', () => {
    expect(readCreationSettings({})).toEqual({
      mode: 'off',
      maxProjectsPerRun: 30,
    })
  })

  test('reads the mode and the cap', () => {
    expect(
      readCreationSettings({
        PCC_PROJECT_CREATION: 'on',
        PCC_PROJECT_CREATION_MAX_PER_RUN: '100',
      }),
    ).toEqual({ mode: 'on', maxProjectsPerRun: 100 })
    expect(readCreationSettings({ PCC_PROJECT_CREATION: 'dry-run' }).mode).toBe('dry-run')
  })

  test('stays off for a value it does not know', () => {
    expect(readCreationSettings({ PCC_PROJECT_CREATION: 'true' }).mode).toBe('off')
  })

  test.each(['abc', '0', '-5', '2.5', ''])('falls back to 30 for the cap "%s"', (raw) => {
    expect(readCreationSettings({ PCC_PROJECT_CREATION_MAX_PER_RUN: raw }).maxProjectsPerRun).toBe(
      30,
    )
  })
})

describe('effectiveCreationMode', () => {
  test.each([
    { mode: 'off', syncIsDryRun: false, hasBackendApi: true, expected: 'off' },
    { mode: 'off', syncIsDryRun: true, hasBackendApi: true, expected: 'off' },
    {
      mode: 'dry-run',
      syncIsDryRun: false,
      hasBackendApi: false,
      expected: 'dry-run',
    },
    { mode: 'on', syncIsDryRun: false, hasBackendApi: true, expected: 'on' },
    {
      mode: 'on',
      syncIsDryRun: true,
      hasBackendApi: true,
      expected: 'dry-run',
    },
    {
      mode: 'on',
      syncIsDryRun: false,
      hasBackendApi: false,
      expected: 'dry-run',
    },
  ] as const)(
    '$mode with sync dry run $syncIsDryRun and backend $hasBackendApi is $expected',
    ({ mode, syncIsDryRun, hasBackendApi, expected }) => {
      expect(effectiveCreationMode(mode, syncIsDryRun, hasBackendApi)).toBe(expected)
    },
  )
})

describe('toProjectToCreate', () => {
  const emptyCdp = buildCdpSegmentIndex([])

  test('turns a creatable project into the requests for the backend', () => {
    const project = zephyrInPcc()
    const plan = planCreation(project.creationTarget, emptyCdp)

    expect(toProjectToCreate(project, plan, emptyCdp)).toEqual({
      pccProjectId: ZEPHYR.pccProjectId,
      pccSlug: ZEPHYR.slug,
      name: ZEPHYR.name,
      status: SegmentStatus.ARCHIVED,
      steps: [{ endpoint: 'projectGroup', node: ZEPHYR }],
    })
  })

  test('returns nothing for a blocked project', () => {
    const project = zephyrInPcc({ creationTarget: null })
    const plan = planCreation(project.creationTarget, emptyCdp)

    expect(toProjectToCreate(project, plan, emptyCdp)).toBeNull()
  })

  test('leaves out a status the backend does not know', () => {
    const project = zephyrInPcc({ status: 'not-a-status' })
    const plan = planCreation(project.creationTarget, emptyCdp)

    expect(toProjectToCreate(project, plan, emptyCdp)?.status).toBeNull()
  })
})

describe('describeBlockedCreation', () => {
  test('describes a blocked project with its reason and the segments in the way', () => {
    const cdp = buildCdpSegmentIndex([
      {
        id: 'segment-1',
        type: SegmentType.PROJECT_GROUP,
        slug: ZEPHYR.slug,
        name: 'Another Zephyr',
        sourceId: null,
        parentId: null,
      },
    ])
    const project = zephyrInPcc()
    const plan = planCreation(project.creationTarget, cdp)

    expect(describeBlockedCreation(project, plan)).toMatchObject({
      name: ZEPHYR.name,
      reason: 'SLUG_OR_NAME_TAKEN',
      segments: [expect.objectContaining({ id: 'segment-1' })],
    })
  })

  test('describes a project whose pcc data is incomplete', () => {
    const project = zephyrInPcc({ creationTarget: null })
    const plan = planCreation(project.creationTarget, buildCdpSegmentIndex([]))

    expect(describeBlockedCreation(project, plan)).toEqual({
      name: ZEPHYR.name,
      reason: 'INCOMPLETE_PCC_DATA',
    })
  })

  test('returns nothing for a project that can be created', () => {
    const project = zephyrInPcc()
    const plan = planCreation(project.creationTarget, buildCdpSegmentIndex([]))

    expect(describeBlockedCreation(project, plan)).toBeNull()
  })
})

describe('createMissingProjects', () => {
  test('calls nothing in a dry run and counts what is left after the cap', async () => {
    const createStep = backendAnswering(() => ({ outcome: 'created' }))
    const recordFailure = failureRecorder()

    const summary = await createMissingProjects(
      [standaloneProject('a'), standaloneProject('b'), standaloneProject('c')],
      { mode: 'dry-run', maxProjects: 2, createStep, recordFailure },
    )

    expect(createStep).not.toHaveBeenCalled()
    expect(recordFailure).not.toHaveBeenCalled()
    expect(summary).toMatchObject({
      mode: 'dry-run',
      creatable: 3,
      created: 0,
      notAttempted: 1,
    })
  })

  test('creates up to the cap and counts the outcomes', async () => {
    const createStep = backendAnswering(() => ({ outcome: 'created' }))

    const summary = await createMissingProjects(
      [standaloneProject('a'), standaloneProject('b'), standaloneProject('c')],
      {
        mode: 'on',
        maxProjects: 2,
        createStep,
        recordFailure: failureRecorder(),
      },
    )

    expect(createStep).toHaveBeenCalledTimes(2)
    expect(summary).toEqual({
      mode: 'on',
      creatable: 3,
      created: 2,
      createdPccProjectIds: ['pcc-a', 'pcc-b'],
      failed: 0,
      failures: [],
      waitingForFailedLevel: 0,
      notAttempted: 1,
      stoppedWithoutAnswers: false,
    })
  })

  test('records a refused project with the level and the answer of the backend', async () => {
    const createStep = backendAnswering((step) =>
      step.node.slug === 'b'
        ? { outcome: 'failed', httpStatus: 400, error: 'HTTP 400: slug taken' }
        : { outcome: 'created' },
    )
    const recordFailure = failureRecorder()

    const summary = await createMissingProjects([standaloneProject('a'), standaloneProject('b')], {
      mode: 'on',
      maxProjects: 10,
      createStep,
      recordFailure,
    })

    expect(summary).toMatchObject({ created: 1, failed: 1 })
    expect(recordFailure).toHaveBeenCalledExactlyOnceWith({
      pccProjectId: 'pcc-b',
      pccSlug: 'b',
      details: {
        name: 'b',
        failedLevel: {
          endpoint: 'projectGroup',
          pccProjectId: 'pcc-b',
          slug: 'b',
        },
        httpStatus: 400,
        error: 'HTTP 400: slug taken',
      },
    })
  })
})

describe('buildCreationFailureAlert', () => {
  const quietRun: MissingProjectCreationSummary = {
    mode: 'on',
    creatable: 3,
    created: 3,
    createdPccProjectIds: ['pcc-a', 'pcc-b', 'pcc-c'],
    failed: 0,
    failures: [],
    waitingForFailedLevel: 0,
    notAttempted: 0,
    stoppedWithoutAnswers: false,
  }
  const refused = (slug: string) => ({
    name: slug,
    endpoint: 'projectGroup' as const,
    slug,
    error: 'HTTP 500: Internal Server Error',
  })

  test('sends nothing when every project was created', () => {
    expect(buildCreationFailureAlert(quietRun)).toBeNull()
  })

  test('sends nothing for a dry run', () => {
    expect(
      buildCreationFailureAlert({
        ...quietRun,
        mode: 'dry-run',
        failed: 1,
        failures: [refused('a')],
      }),
    ).toBeNull()
  })

  test('lists the projects that failed with the level and the answer of the backend', () => {
    const alert = buildCreationFailureAlert({
      ...quietRun,
      created: 2,
      failed: 1,
      failures: [refused('bluesky')],
    })

    expect(alert?.title).toBe('PCC sync could not create 1 project')
    expect(alert?.text).toContain('*Created:* 2 | *Failed:* 1')
    expect(alert?.text).toContain(
      '• *bluesky* (projectGroup `bluesky`): HTTP 500: Internal Server Error',
    )
  })

  test('escapes names and cuts long answers', () => {
    const alert = buildCreationFailureAlert({
      ...quietRun,
      failed: 1,
      failures: [{ ...refused('qhttpx'), name: '<QHTTPX>', error: 'x'.repeat(500) }],
    })

    expect(alert?.text).toContain('*&lt;QHTTPX&gt;*')
    expect(alert?.text).toContain(`${'x'.repeat(200)}…`)
    expect(alert?.text).not.toContain('x'.repeat(201))
  })

  test('keeps a multi-line answer on the line of its project', () => {
    const alert = buildCreationFailureAlert({
      ...quietRun,
      failed: 1,
      failures: [
        {
          ...refused('bluesky'),
          error: 'HTTP 502: <html>\n  <body>Bad Gateway</body>\n</html>',
        },
      ],
    })

    expect(alert?.text).toContain(
      '• *bluesky* (projectGroup `bluesky`): HTTP 502: &lt;html&gt; &lt;body&gt;Bad Gateway&lt;/body&gt; &lt;/html&gt;',
    )
  })

  test('lists at most 20 failures and counts the rest', () => {
    const failures = Array.from({ length: 25 }, (_, i) => refused(`p${i}`))

    const alert = buildCreationFailureAlert({
      ...quietRun,
      failed: 25,
      failures,
    })

    expect(alert?.title).toBe('PCC sync could not create 25 projects')
    expect(alert?.text.match(/^• /gm)).toHaveLength(20)
    expect(alert?.text).toContain('… and 5 more.')
  })

  test('warns when the run stopped because the backend did not answer', () => {
    const alert = buildCreationFailureAlert({
      ...quietRun,
      created: 0,
      failed: 3,
      failures: [refused('a'), refused('b'), refused('c')],
      notAttempted: 3,
      stoppedWithoutAnswers: true,
    })

    expect(alert?.title).toBe('PCC sync stopped creating projects: the backend is not answering')
    expect(alert?.text).toContain('stopped early because the backend was not answering')
  })
})
