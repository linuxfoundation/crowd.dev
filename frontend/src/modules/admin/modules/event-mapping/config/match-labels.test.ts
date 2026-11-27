// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from 'vitest';
import type {
  EventMappingCventEvent,
  EventMappingMatch,
  EventMappingProject,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';
import {
  manualPickBadge,
  manualPickEvidence,
  matchBadge,
  matchEvidence,
} from '@/modules/admin/modules/event-mapping/config/match-labels';

const cvent = (dayGap: number): EventMappingCventEvent => ({
  name: 'ExampleCon Europe 2027',
  startDate: '2027-03-10',
  dayGap,
});

const exampleProject: EventMappingProject = {
  segmentId: '4f2b8c1e-7a3d-4e5f-9b6a-1c2d3e4f5a6b',
  name: 'Example Project',
  groupName: 'Example Foundation',
};

const evidence = (match: EventMappingMatch, cventEvent: EventMappingCventEvent | null = null) =>
  matchEvidence({ match, cventEvent });

describe('matchEvidence', () => {
  it.each([
    [0, 'Same name as Cvent: ExampleCon Europe 2027 · same start date'],
    [1, 'Same name as Cvent: ExampleCon Europe 2027 · start 1 day apart'],
    [3, 'Same name as Cvent: ExampleCon Europe 2027 · start 3 days apart'],
  ])('names the Cvent event and its start gap of %i days for same_name', (dayGap, text) => {
    expect(evidence('same_name', cvent(dayGap))).toBe(text);
  });

  it('describes every match with its Cvent event', () => {
    expect(evidence('similar_name', cvent(1))).toBe(
      'Similar to Cvent: ExampleCon Europe 2027 · start 1 day apart, clear lead',
    );
    expect(evidence('name_contained', cvent(3))).toBe(
      'Name contained in Cvent: ExampleCon Europe 2027 · start 3 days apart',
    );
    expect(evidence('sub_event', cvent(0))).toBe(
      'Sub-event of ExampleCon Europe 2027, mapped to its project',
    );
    expect(evidence('ambiguous', cvent(2))).toBe(
      "Closest Cvent event is ExampleCon Europe 2027. The names differ, so the rules don't accept it.",
    );
    expect(evidence('project_not_in_cdp', cvent(0))).toBe(
      'The Cvent event ExampleCon Europe 2027 belongs to a project that is not in CDP.',
    );
  });

  it('describes every match that has no Cvent event', () => {
    expect(evidence('project_alias')).toBe('The event name points to this project');
    expect(evidence('ambiguous')).toBe('Several Cvent events have this name.');
    expect(evidence('no_cvent_event')).toBe(
      'CDP only knows a Cvent event once it has registrations.',
    );
    expect(evidence('not_checked')).toBe('The next daily matching run checks this event.');
    expect(evidence('main_event_not_in_cdp')).toBe(
      'Sub-event whose main event has no Cvent event in CDP.',
    );
    expect(evidence('project_not_in_cdp')).toBe('The project this event points to is not in CDP.');
  });
});

describe('matchBadge', () => {
  it.each<[EventMappingMatch, string]>([
    ['same_name', 'Best match'],
    ['similar_name', 'Best match'],
    ['name_contained', 'Best match'],
    ['project_alias', 'Best match'],
    ['sub_event', 'Best match · sub-event'],
    ['ambiguous', 'Ambiguous'],
    ['no_cvent_event', 'No Cvent event in CDP'],
    ['not_checked', 'Not checked yet'],
    ['main_event_not_in_cdp', 'Main event not in CDP'],
    ['project_not_in_cdp', 'Project not in CDP'],
  ])('labels %s as %s', (match, label) => {
    expect(matchBadge(match).label).toBe(label);
  });

  it('colours best matches amber, sub-events blue and unresolved reasons orange', () => {
    expect(matchBadge('similar_name').colorClass).toBe(matchBadge('project_alias').colorClass);
    expect(matchBadge('sub_event').colorClass).not.toBe(matchBadge('similar_name').colorClass);
    expect(matchBadge('ambiguous').colorClass).toBe(matchBadge('not_checked').colorClass);
    expect(matchBadge('ambiguous').colorClass).not.toBe(matchBadge('similar_name').colorClass);
    expect(manualPickBadge.colorClass).not.toBe(matchBadge('ambiguous').colorClass);
  });
});

describe('manual pick', () => {
  it('names the rule suggestion it replaces', () => {
    expect(manualPickBadge.label).toBe('Picked manually');
    expect(manualPickEvidence({ match: 'similar_name', suggestedProject: exampleProject })).toBe(
      'Replaces the rule suggestion: Example Project',
    );
  });

  it('names the reason the rules found no match', () => {
    expect(manualPickEvidence({ match: 'ambiguous', suggestedProject: null })).toBe(
      'Rules found no match: Ambiguous',
    );
  });
});
