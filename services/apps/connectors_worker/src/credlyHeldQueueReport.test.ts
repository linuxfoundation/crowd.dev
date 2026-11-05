import { describe, expect, it } from 'vitest'

import type {
  ICredlyHeldTemplate,
  ICredlyStoredTemplateMapping,
} from '@crowd/data-access-layer/src/credly'

import { HELD_QUEUE_TOP, activeHeld, buildHeldQueueReport } from './credlyHeldQueueReport'
import type { PlanEntry, ResolvePlan } from './credlyTemplateResolve'

function held(id: string, overrides: Partial<ICredlyHeldTemplate> = {}): ICredlyHeldTemplate {
  return {
    badgeTemplateId: id,
    badgeTemplateName: id,
    badgeCount: 1,
    holdReason: 'no_match',
    projectSlug: null,
    rule: 'hold',
    lastSeenAt: '2026-10-28 12:00:00+00',
    ...overrides,
  }
}

function stored(
  id: string,
  overrides: Partial<ICredlyStoredTemplateMapping> = {},
): ICredlyStoredTemplateMapping {
  return {
    badgeTemplateId: id,
    badgeTemplateName: id,
    status: 'held',
    holdReason: 'no_match',
    projectSlug: null,
    segmentId: null,
    integrationId: null,
    rule: 'hold',
    ruleVersion: 1,
    matchedSku: null,
    evidence: null,
    reviewedBy: null,
    lockedAt: null,
    deactivatedAt: null,
    ...overrides,
  }
}

function plan(entries: Partial<PlanEntry>[]): ResolvePlan {
  return {
    entries: entries.map((entry) => ({
      kind: 'unchanged' as const,
      badgeTemplateId: 'x',
      badgeTemplateName: 'x',
      before: null,
      after: null,
      ...entry,
    })),
    counts: { new: 0, unchanged: 0, changed: 0, locked: 0, missing: 0 },
  }
}

describe('activeHeld', () => {
  it('keeps only live held rows', () => {
    expect(
      activeHeld([
        stored('held'),
        stored('mapped', { status: 'mapped', holdReason: null }),
        stored('gone', { deactivatedAt: new Date() }),
      ]).map((row) => row.badgeTemplateId),
    ).toEqual(['held'])
  })
})

describe('buildHeldQueueReport', () => {
  it('summarises the stored queue, lists what entered and left it, and the top by badges', () => {
    const queue = [
      held('lfc131', {
        badgeTemplateName: 'LFC131: Green Software & Practitioners',
        badgeCount: 87073,
        holdReason: 'no_project',
      }),
      held('free5gc', {
        badgeTemplateName: 'LFS114: Introduction to free5GC',
        badgeCount: 2000,
        holdReason: 'no_segment',
        projectSlug: 'free5gc',
      }),
      held('speaker', { badgeTemplateName: 'KubeCon <Speaker>', badgeCount: 300 }),
      held('back', { badgeTemplateName: 'Back again', badgeCount: 50 }),
    ]
    const report = buildHeldQueueReport(
      [
        stored('lfc131'),
        stored('free5gc'),
        stored('cka', { badgeTemplateName: 'CKA' }),
        stored('gone', { badgeTemplateName: 'Gone' }),
      ],
      queue,
      plan([
        { badgeTemplateId: 'cka', kind: 'changed', before: 'held:no_segment', after: 'mapped:k8s' },
        { badgeTemplateId: 'gone', kind: 'missing', before: 'held:no_match', after: null },
      ]),
      new Map([['lfc131', 'https://www.credly.com/org/the-linux-foundation/badge/lfc131']]),
    )

    expect(report.changed).toBe(true)
    expect(report.title).toBe('Credly held templates: 2 new, 2 released')
    expect(report.sections.map((section) => section.title)).toEqual([
      'Held queue',
      'Newly held (2)',
      'Released (2)',
      'Top 4 by badges',
      'To resolve one',
    ])
    expect(report.sections[0].text).toBe(
      '4 templates, 89,423 badges not ingested.\n`no_match` 2 · `no_project` 1 · `no_segment` 1',
    )
    expect(report.sections[1].text).toBe(
      '• KubeCon &lt;Speaker&gt; — 300 badges, `no_match`, id `speaker`\n• Back again — 50 badges, `no_match`, id `back`',
    )
    expect(report.sections[2].text).toBe('• CKA → mapped:k8s\n• Gone → gone from Credly')
    expect(report.sections[3].text.split('\n')[0]).toBe(
      '• <https://www.credly.com/org/the-linux-foundation/badge/lfc131|LFC131: Green Software &amp; Practitioners> — 87,073 badges, `no_project`, id `lfc131`',
    )
    expect(report.sections[3].text.split('\n')[1]).toContain('`no_segment`, free5gc, id `free5gc`')
  })

  it('counts a reactivated template as newly held, by membership and not by label', () => {
    const report = buildHeldQueueReport(
      activeHeld([stored('back', { deactivatedAt: new Date() })]),
      [held('back')],
      plan([
        {
          badgeTemplateId: 'back',
          kind: 'changed',
          before: 'held:no_match',
          after: 'held:no_match',
        },
      ]),
      new Map(),
    )

    expect(report.changed).toBe(true)
    expect(report.title).toBe('Credly held templates: 1 new, 0 released')
  })

  it('reports no change when nothing entered or left the queue', () => {
    const report = buildHeldQueueReport(
      [stored('a')],
      [held('a', { badgeCount: 5 })],
      plan([{ badgeTemplateId: 'a', before: 'held:no_match', after: 'held:no_match' }]),
      new Map(),
    )

    expect(report.changed).toBe(false)
    expect(report.title).toBe('Credly held templates: 1 waiting for review')
    expect(report.sections.map((section) => section.title)).toEqual([
      'Held queue',
      'Top 1 by badges',
      'To resolve one',
    ])
  })

  it('keeps an empty queue to one line', () => {
    const report = buildHeldQueueReport([], [], plan([]), new Map())

    expect(report.sections).toEqual([
      { title: 'Held queue', text: '0 templates, 0 badges not ingested.' },
    ])
    expect(report.title).toBe('Credly held templates: 0 waiting for review')
  })

  it('caps the lists at the top entries and says how many more there are', () => {
    const queue = Array.from({ length: HELD_QUEUE_TOP + 5 }, (_, i) =>
      held(`t-${i}`, { badgeCount: i }),
    )
    const report = buildHeldQueueReport([], queue, plan([]), new Map())

    const newly = report.sections[1].text.split('\n')
    expect(report.sections[1].title).toBe(`Newly held (${HELD_QUEUE_TOP + 5})`)
    expect(newly).toHaveLength(HELD_QUEUE_TOP + 1)
    expect(newly[0]).toContain(`t-${HELD_QUEUE_TOP + 4}`)
    expect(newly[HELD_QUEUE_TOP]).toBe('… and 5 more')
  })
})
