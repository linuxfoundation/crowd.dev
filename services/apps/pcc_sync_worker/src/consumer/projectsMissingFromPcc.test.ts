import { describe, expect, test } from 'vitest'

import { SegmentType } from '@crowd/types'

import { buildCdpSegmentIndex } from './creationPlan'
import type { CdpSegment } from './creationPlan'
import { addPccProjectIdsOfRow, findProjectsMissingFromPcc } from './projectsMissingFromPcc'

const KUBERNETES = 'a092M00001IV42DQAT'
const CNCF = 'a0941000002wBz4AAE'
const DPDK_TOOLS = 'a092M00001JWs6MQAT'
const LANGUAGE_INTEROP = 'lfv8NMKI8tcp96N5tb'

function segment(type: SegmentType, slug: string, sourceId: string | null): CdpSegment {
  return { id: `${type}-${slug}`, type, slug, name: slug, sourceId, parentId: null }
}

function subproject(slug: string, sourceId: string | null): CdpSegment {
  return segment(SegmentType.SUB_PROJECT, slug, sourceId)
}

function exportedIds(...rows: Record<string, unknown>[]): Set<string> {
  const pccProjectIds = new Set<string>()
  for (const row of rows) addPccProjectIdsOfRow(pccProjectIds, row)
  return pccProjectIds
}

describe('addPccProjectIdsOfRow', () => {
  test('collects the leaf and its ancestor', () => {
    const ids = exportedIds({ PROJECT_ID: KUBERNETES, MAPPED_PROJECT_ID: CNCF })

    expect([...ids].sort()).toEqual([CNCF, KUBERNETES].sort())
  })

  test('trims the ids and skips the empty ones', () => {
    const ids = exportedIds({ PROJECT_ID: ` ${KUBERNETES} `, MAPPED_PROJECT_ID: null })

    expect([...ids]).toEqual([KUBERNETES])
  })
})

describe('findProjectsMissingFromPcc', () => {
  test('reports a sub-project whose pcc project is not in the export', () => {
    const index = buildCdpSegmentIndex([
      subproject('dpdk-tools', DPDK_TOOLS),
      subproject('k8s', KUBERNETES),
    ])

    const missing = findProjectsMissingFromPcc(index, exportedIds({ PROJECT_ID: KUBERNETES }))

    expect(missing).toEqual([
      {
        pccProjectId: DPDK_TOOLS,
        subprojects: [{ id: 'subproject-dpdk-tools', name: 'dpdk-tools', slug: 'dpdk-tools' }],
      },
    ])
  })

  test('does not report a parent that the export only lists as an ancestor', () => {
    const index = buildCdpSegmentIndex([subproject('cncf', CNCF)])

    const missing = findProjectsMissingFromPcc(
      index,
      exportedIds({ PROJECT_ID: KUBERNETES, MAPPED_PROJECT_ID: CNCF }),
    )

    expect(missing).toEqual([])
  })

  test('accepts both shapes of pcc project id', () => {
    const index = buildCdpSegmentIndex([
      subproject('dpdk-tools', DPDK_TOOLS),
      subproject('aswf-language-interop-project', LANGUAGE_INTEROP),
    ])

    const missing = findProjectsMissingFromPcc(index, exportedIds({ PROJECT_ID: KUBERNETES }))

    expect(missing.map((project) => project.pccProjectId).sort()).toEqual(
      [DPDK_TOOLS, LANGUAGE_INTEROP].sort(),
    )
  })

  test('ignores source ids that are not pcc project ids', () => {
    const index = buildCdpSegmentIndex([
      subproject('nonlf_angular', 'Angular'),
      subproject('deprecated_rune-dsl', 'deprecated_a092M00001L16QiQAJ'),
      subproject('deltalake_delete', 'a092M00001JWr8RQAT_delete'),
      subproject('unlinked', null),
    ])

    const missing = findProjectsMissingFromPcc(index, exportedIds({ PROJECT_ID: KUBERNETES }))

    expect(missing).toEqual([])
  })

  test('ignores a pcc id that sits only on a project or a project group', () => {
    const index = buildCdpSegmentIndex([
      segment(SegmentType.PROJECT_GROUP, 'dpdk', DPDK_TOOLS),
      segment(SegmentType.PROJECT, 'dpdk', DPDK_TOOLS),
    ])

    const missing = findProjectsMissingFromPcc(index, exportedIds({ PROJECT_ID: KUBERNETES }))

    expect(missing).toEqual([])
  })

  test('reports one entry per pcc id when several sub-projects share it', () => {
    const index = buildCdpSegmentIndex([
      subproject('dpdk-tools', DPDK_TOOLS),
      subproject('dpdk-tools-copy', DPDK_TOOLS),
      segment(SegmentType.PROJECT, 'dpdk-tools', DPDK_TOOLS),
    ])

    const missing = findProjectsMissingFromPcc(index, exportedIds({ PROJECT_ID: KUBERNETES }))

    expect(missing).toHaveLength(1)
    expect(missing[0].subprojects.map((cdp) => cdp.slug)).toEqual(['dpdk-tools', 'dpdk-tools-copy'])
  })

  test('reports nothing when the export is empty', () => {
    const index = buildCdpSegmentIndex([subproject('dpdk-tools', DPDK_TOOLS)])

    expect(findProjectsMissingFromPcc(index, new Set())).toEqual([])
  })
})
