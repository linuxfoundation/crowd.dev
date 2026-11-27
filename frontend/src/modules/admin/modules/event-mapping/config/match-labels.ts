// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type {
  EventMappingMatch,
  EventMappingRow,
  MatchBadge,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';

const MATCH_BADGES: Record<EventMappingMatch, MatchBadge> = {
  same_name: { label: 'Best match', pillType: 'primary' },
  similar_name: { label: 'Best match', pillType: 'primary' },
  name_contained: { label: 'Best match', pillType: 'primary' },
  project_alias: { label: 'Best match', pillType: 'primary' },
  sub_event: { label: 'Best match · sub-event', pillType: 'primary' },
  ambiguous: { label: 'Ambiguous', pillType: 'warning' },
  no_cvent_event: { label: 'No Cvent event in CDP', pillType: 'warning' },
  not_checked: { label: 'Not checked yet', pillType: 'warning' },
  main_event_not_in_cdp: { label: 'Main event not in CDP', pillType: 'warning' },
  project_not_in_cdp: { label: 'Project not in CDP', pillType: 'warning' },
};

export const manualPickBadge: MatchBadge = { label: 'Picked manually', pillType: 'secondary' };

export const matchBadge = (match: EventMappingMatch): MatchBadge => MATCH_BADGES[match];

const startGap = (dayGap: number): string => {
  if (dayGap === 0) {
    return 'same start date';
  }
  return dayGap === 1 ? 'start 1 day apart' : `start ${dayGap} days apart`;
};

const evidenceWithoutCventEvent: Record<EventMappingMatch, string> = {
  same_name: 'Same name as a Cvent event',
  similar_name: 'Similar to a Cvent event',
  name_contained: 'Name contained in a Cvent event',
  sub_event: 'Sub-event mapped to its project',
  project_alias: 'The event name points to this project',
  ambiguous: 'Several Cvent events have this name.',
  no_cvent_event: 'CDP only knows a Cvent event once it has registrations.',
  not_checked: 'The next daily matching run checks this event.',
  main_event_not_in_cdp: 'Sub-event whose main event has no Cvent event in CDP.',
  project_not_in_cdp: 'The project this event points to is not in CDP.',
};

export const matchEvidence = ({
  match,
  cventEvent,
}: Pick<EventMappingRow, 'match' | 'cventEvent'>): string => {
  if (cventEvent === null) {
    return evidenceWithoutCventEvent[match];
  }
  const { name, dayGap } = cventEvent;
  switch (match) {
    case 'same_name':
      return `Same name as Cvent: ${name} · ${startGap(dayGap)}`;
    case 'similar_name':
      return `Similar to Cvent: ${name} · ${startGap(dayGap)}, clear lead`;
    case 'name_contained':
      return `Name contained in Cvent: ${name} · ${startGap(dayGap)}`;
    case 'sub_event':
      return `Sub-event of ${name}, mapped to its project`;
    case 'ambiguous':
      return `Closest Cvent event is ${name}. The names differ, so the rules don't accept it.`;
    case 'project_not_in_cdp':
      return `The Cvent event ${name} belongs to a project that is not in CDP.`;
    default:
      return evidenceWithoutCventEvent[match];
  }
};

export const manualPickEvidence = ({
  match,
  suggestedProject,
}: Pick<EventMappingRow, 'match' | 'suggestedProject'>): string =>
  suggestedProject
    ? `Replaces the rule suggestion: ${suggestedProject.name}`
    : `Rules found no match: ${matchBadge(match).label}`;
