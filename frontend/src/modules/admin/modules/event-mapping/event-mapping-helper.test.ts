// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from 'vitest';
import type {
  EventMappingProject,
  EventMappingRow,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';
import {
  applyConfirmed,
  applyIgnored,
  applyRestored,
  confirmations,
  displayedProject,
  formatEventDates,
  groupProjects,
  keepPendingPicks,
  keepSelectable,
  pendingCount,
  pickProject,
  rowBadge,
  rowEvidence,
  rowsInTab,
  showsHint,
  statusCounts,
  toConfirmItems,
} from '@/modules/admin/modules/event-mapping/event-mapping-helper';

const project = (segmentId: string, name: string): EventMappingProject => ({
  segmentId,
  name,
  groupName: 'Example Foundation',
});

const SUGGESTED = project('11111111-1111-4111-8111-111111111111', 'Example Project');
const OTHER = project('22222222-2222-4222-8222-222222222222', 'Other Project');

const row = (eventKey: string, overrides: Partial<EventMappingRow> = {}): EventMappingRow => ({
  platform: 'sessionize',
  eventKey,
  eventName: `ExampleCon ${eventKey}`,
  eventStart: '2027-03-10',
  eventEnd: '2027-03-11',
  state: 'suggested',
  match: 'similar_name',
  cventEvent: { name: 'ExampleCon 2027', startDate: '2027-03-10', dayGap: 0 },
  suggestedProject: SUGGESTED,
  hintProject: null,
  project: null,
  decidedAt: null,
  canUndo: false,
  ...overrides,
});

const unresolved = (eventKey: string, overrides: Partial<EventMappingRow> = {}) =>
  row(eventKey, {
    state: 'unresolved',
    match: 'ambiguous',
    cventEvent: null,
    suggestedProject: null,
    ...overrides,
  });

const noPicks = new Map<string, EventMappingProject>();

describe('tabs and counts', () => {
  const rows = [
    row('a'),
    unresolved('b'),
    row('c', { state: 'confirmed', project: SUGGESTED }),
    row('d', { state: 'ignored' }),
    unresolved('e'),
  ];

  it('lists only the rows of the selected status', () => {
    expect(rowsInTab(rows, 'all', 'unresolved').map(({ eventKey }) => eventKey)).toEqual([
      'b',
      'e',
    ]);
    expect(rowsInTab(rows, 'sessionize', 'confirmed').map(({ eventKey }) => eventKey)).toEqual([
      'c',
    ]);
  });

  it('counts the rows of each status and the pending ones for the tab badge', () => {
    const counts = statusCounts(rows, 'all');
    expect(counts).toEqual({ suggested: 1, unresolved: 2, confirmed: 1, ignored: 1 });
    expect(pendingCount(counts)).toBe(3);
  });
});

describe('picked projects', () => {
  it('keeps a manual pick and drops it when the suggestion is picked again', () => {
    const suggested = row('a');
    const picked = pickProject(noPicks, suggested, OTHER);
    expect(displayedProject(suggested, picked)).toEqual(OTHER);
    expect(rowBadge(suggested, picked).label).toBe('Picked manually');
    expect(rowEvidence(suggested, picked)).toBe('Replaces the rule suggestion: Example Project');

    const back = pickProject(picked, suggested, SUGGESTED);
    expect(back.has('a')).toBe(false);
    expect(rowBadge(suggested, back).label).toBe('Best match');
  });

  it('shows the hint button until the hint project is picked', () => {
    const hinted = unresolved('b', { hintProject: OTHER });
    expect(showsHint(hinted, noPicks)).toBe(true);
    const picked = pickProject(noPicks, hinted, OTHER);
    expect(showsHint(hinted, picked)).toBe(false);
    expect(rowEvidence(hinted, picked)).toBe('Rules found no match: Ambiguous');
  });

  it('forgets picks of rows that are no longer pending', () => {
    const picked = pickProject(pickProject(noPicks, row('a'), OTHER), unresolved('b'), OTHER);
    const kept = keepPendingPicks(picked, [
      row('a', { state: 'confirmed', project: OTHER }),
      unresolved('b'),
    ]);
    expect([...kept.keys()]).toEqual(['b']);
  });

  it('marks a confirmed row whose project differs from the suggestion as picked manually', () => {
    expect(rowBadge(row('a', { state: 'confirmed', project: OTHER }), noPicks).label).toBe(
      'Picked manually',
    );
    expect(rowBadge(row('a', { state: 'confirmed', project: SUGGESTED }), noPicks).label).toBe(
      'Best match',
    );
  });
});

describe('selection and bulk confirm', () => {
  const rows = [row('a'), unresolved('b'), unresolved('c'), row('d', { state: 'ignored' })];

  it('keeps only pending rows that have a project', () => {
    const picked = pickProject(noPicks, unresolved('c'), OTHER);
    const selected = keepSelectable(new Set(['a', 'b', 'c', 'd']), rows, picked);
    expect([...selected]).toEqual(['a', 'c']);
  });

  it('sends every selected row with its picked or suggested segment', () => {
    const picked = pickProject(noPicks, unresolved('c'), OTHER);
    expect(toConfirmItems(confirmations(rows, new Set(['a', 'c']), picked))).toEqual([
      { platform: 'sessionize', eventKey: 'a', segmentId: SUGGESTED.segmentId },
      { platform: 'sessionize', eventKey: 'c', segmentId: OTHER.segmentId },
    ]);
  });
});

describe('formatEventDates', () => {
  it.each([
    ['2026-05-17', '2026-05-17', 'May 17, 2026'],
    ['2024-09-11', '2024-09-12', 'Sep 11–12, 2024'],
    ['2025-08-30', '2025-09-02', 'Aug 30 – Sep 2, 2025'],
    ['2025-12-30', '2026-01-02', 'Dec 30, 2025 – Jan 2, 2026'],
  ])('formats %s to %s as %s', (start, end, text) => {
    expect(formatEventDates(start, end)).toBe(text);
  });
});

describe('groupProjects', () => {
  it('groups projects under their project group, groups sorted by name', () => {
    expect(
      groupProjects([
        { id: '1', name: 'Kubernetes', grandparentName: 'CNCF' },
        { id: '2', name: 'Open Source Summit', grandparentName: 'The Linux Foundation' },
        { id: '3', name: 'Prometheus', grandparentName: 'CNCF' },
      ]),
    ).toEqual([
      {
        groupName: 'CNCF',
        projects: [
          { segmentId: '1', name: 'Kubernetes', groupName: 'CNCF' },
          { segmentId: '3', name: 'Prometheus', groupName: 'CNCF' },
        ],
      },
      {
        groupName: 'The Linux Foundation',
        projects: [
          { segmentId: '2', name: 'Open Source Summit', groupName: 'The Linux Foundation' },
        ],
      },
    ]);
  });
});

describe('applying decisions to the cached list', () => {
  const NOW = '2026-11-27T12:00:00.000Z';
  const list = () => {
    const rows = [
      row('a'),
      unresolved('b'),
      row('c', { state: 'confirmed', project: SUGGESTED, decidedAt: NOW, canUndo: true }),
      unresolved('d', { state: 'ignored', decidedAt: NOW, canUndo: true }),
    ];
    return { rows, counts: statusCounts(rows, 'all') };
  };
  const stateOf = (value: ReturnType<typeof list>, eventKey: string) =>
    value.rows.find((r) => r.eventKey === eventKey);

  it('confirms one row with its project and moves it between counts', () => {
    const next = applyConfirmed(
      list(),
      [{ platform: 'sessionize', eventKey: 'a', project: OTHER }],
      NOW,
    );
    expect(stateOf(next, 'a')).toMatchObject({
      state: 'confirmed',
      project: OTHER,
      decidedAt: NOW,
      canUndo: true,
    });
    expect(next.counts).toEqual({ suggested: 0, unresolved: 1, confirmed: 2, ignored: 1 });
  });

  it('confirms every row of a bulk confirm', () => {
    const next = applyConfirmed(
      list(),
      [
        { platform: 'sessionize', eventKey: 'a', project: SUGGESTED },
        { platform: 'sessionize', eventKey: 'b', project: OTHER },
      ],
      NOW,
    );
    expect(stateOf(next, 'b')).toMatchObject({ state: 'confirmed', project: OTHER });
    expect(next.counts).toEqual({ suggested: 0, unresolved: 0, confirmed: 3, ignored: 1 });
  });

  it('ignores a row', () => {
    const next = applyIgnored(list(), 'b', NOW);
    expect(stateOf(next, 'b')).toMatchObject({
      state: 'ignored',
      project: null,
      decidedAt: NOW,
      canUndo: true,
    });
    expect(next.counts).toEqual({ suggested: 1, unresolved: 0, confirmed: 1, ignored: 2 });
  });

  it('restores a row to suggested when it has a suggestion, else to unresolved', () => {
    const undone = applyRestored(list(), 'c');
    expect(stateOf(undone, 'c')).toMatchObject({
      state: 'suggested',
      project: null,
      decidedAt: null,
      canUndo: false,
    });
    const restored = applyRestored(undone, 'd');
    expect(stateOf(restored, 'd')).toMatchObject({ state: 'unresolved', decidedAt: null });
    expect(restored.counts).toEqual({ suggested: 2, unresolved: 2, confirmed: 0, ignored: 0 });
  });
});
