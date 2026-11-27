// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from 'vitest';
import type {
  EventMappingProject,
  EventMappingRow,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';
import {
  confirmItems,
  decisionNote,
  keepDecidedNow,
  displayedProject,
  formatEventDates,
  groupProjects,
  keepKnownPicks,
  keepSelectable,
  listState,
  pendingCount,
  pickProject,
  rowBadge,
  rowEvidence,
  rowsInTab,
  showsHint,
  statusCounts,
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

  it('keeps a pick across confirm, undo and ignore, and forgets it once the row is gone', () => {
    const picked = pickProject(noPicks, row('a'), OTHER);
    const confirmed = row('a', { state: 'confirmed', project: OTHER });
    const undone = row('a');
    const ignored = row('a', { state: 'ignored' });

    expect(keepKnownPicks(picked, [confirmed]).get('a')).toEqual(OTHER);
    expect(displayedProject(undone, keepKnownPicks(picked, [undone]))).toEqual(OTHER);
    expect(rowBadge(undone, picked).label).toBe('Picked manually');
    expect(displayedProject(ignored, picked)).toEqual(OTHER);
    expect(rowBadge(ignored, picked).label).toBe('Picked manually');
    expect(keepKnownPicks(picked, [row('b')]).size).toBe(0);
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
    expect(confirmItems(rows, new Set(['a', 'c']), picked)).toEqual([
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
  it('groups server-ordered projects into contiguous groups without re-sorting', () => {
    const cncf = (segmentId: string, name: string) => ({ segmentId, name, groupName: 'CNCF' });
    const lf = { segmentId: '3', name: 'Open Source Summit', groupName: 'The Linux Foundation' };
    expect(groupProjects([cncf('1', 'Kubernetes'), cncf('2', 'Argo'), lf])).toEqual([
      { groupName: 'CNCF', projects: [cncf('1', 'Kubernetes'), cncf('2', 'Argo')] },
      { groupName: 'The Linux Foundation', projects: [lf] },
    ]);
  });
});

describe('decisions made on this page', () => {
  const decidedAt = '2026-11-12T10:00:00Z';
  const decided = row('a', { state: 'confirmed', project: SUGGESTED, decidedAt });

  it('says "Just now" for a decision made on this page, else shows the decision date', () => {
    expect(decisionNote(decided, new Map([['a', decidedAt]]))).toBe('Just now');
    expect(decisionNote(decided, new Map())).toBe('Nov 12, 2026');
    expect(decisionNote(row('b'), new Map())).toBe('');
  });

  it('records the decision date the refetch returns for a new decision', () => {
    expect(keepDecidedNow(new Map([['a', null]]), [decided])).toEqual(new Map([['a', decidedAt]]));
  });

  it('forgets a decision once the row is pending again, re-decided elsewhere, or gone', () => {
    const recorded = new Map([['a', decidedAt]]);
    expect(keepDecidedNow(recorded, [row('a')]).size).toBe(0);
    expect(keepDecidedNow(recorded, [{ ...decided, decidedAt: '2026-11-13T10:00:00Z' }]).size).toBe(
      0,
    );
    expect(keepDecidedNow(recorded, []).size).toBe(0);
    expect(keepDecidedNow(recorded, [decided])).toEqual(recorded);
  });
});

describe('listState', () => {
  it('tells a failed first load from a failed refresh of a list already shown', () => {
    expect(listState({ hasData: false, hasError: true })).toBe('load-failed');
    expect(listState({ hasData: true, hasError: true })).toBe('refresh-failed');
    expect(listState({ hasData: true, hasError: false })).toBe('ready');
    expect(listState({ hasData: false, hasError: false })).toBe('loading');
  });
});
