// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { dateHelper } from '@/shared/date-helper/date-helper';
import type { MatchBadge } from '@/modules/admin/modules/event-mapping/config/match-labels';
import {
  manualPickBadge,
  manualPickEvidence,
  matchBadge,
  matchEvidence,
} from '@/modules/admin/modules/event-mapping/config/match-labels';
import type {
  EventMappingConfirmItem,
  EventMappingPlatform,
  EventMappingProject,
  EventMappingRow,
  EventMappingState,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';

export type EventMappingPlatformFilter = 'all' | EventMappingPlatform;
export type PickedProjects = ReadonlyMap<string, EventMappingProject>;
export type StatusCounts = Record<EventMappingState, number>;

export interface ProjectGroupOption {
  groupName: string;
  projects: EventMappingProject[];
}

export interface PickerSubproject {
  id: string;
  name: string;
  grandparentName: string;
}

export const isPending = ({ state }: EventMappingRow): boolean =>
  state === 'suggested' || state === 'unresolved';

const inPlatform = (rows: EventMappingRow[], platform: EventMappingPlatformFilter) =>
  rows.filter((row) => platform === 'all' || row.platform === platform);

export const rowsInTab = (
  rows: EventMappingRow[],
  platform: EventMappingPlatformFilter,
  state: EventMappingState,
): EventMappingRow[] => inPlatform(rows, platform).filter((row) => row.state === state);

export const statusCounts = (
  rows: EventMappingRow[],
  platform: EventMappingPlatformFilter,
): StatusCounts =>
  inPlatform(rows, platform).reduce<StatusCounts>(
    (counts, { state }) => ({ ...counts, [state]: counts[state] + 1 }),
    {
      suggested: 0,
      unresolved: 0,
      confirmed: 0,
      ignored: 0,
    },
  );

export const pendingCount = (counts: StatusCounts): number => counts.suggested + counts.unresolved;

export const reviewedCount = (counts: StatusCounts): number => counts.confirmed + counts.ignored;

export const displayedProject = (
  row: EventMappingRow,
  picked: PickedProjects,
): EventMappingProject | null => {
  if (isPending(row)) {
    return picked.get(row.eventKey) ?? row.suggestedProject;
  }
  return row.project ?? row.suggestedProject;
};

const isManualPick = (row: EventMappingRow, picked: PickedProjects): boolean => {
  if (isPending(row)) {
    return picked.has(row.eventKey);
  }
  return row.state === 'confirmed' && row.project?.segmentId !== row.suggestedProject?.segmentId;
};

export const rowBadge = (row: EventMappingRow, picked: PickedProjects): MatchBadge =>
  isManualPick(row, picked) ? manualPickBadge : matchBadge(row.match);

export const rowEvidence = (row: EventMappingRow, picked: PickedProjects): string =>
  isManualPick(row, picked) ? manualPickEvidence(row) : matchEvidence(row);

export const showsHint = (row: EventMappingRow, picked: PickedProjects): boolean =>
  isPending(row) &&
  row.hintProject !== null &&
  displayedProject(row, picked)?.segmentId !== row.hintProject.segmentId;

export const isSelectable = (row: EventMappingRow, picked: PickedProjects): boolean =>
  isPending(row) && displayedProject(row, picked) !== null;

export const pickProject = (
  picked: PickedProjects,
  row: EventMappingRow,
  project: EventMappingProject,
): Map<string, EventMappingProject> => {
  const next = new Map(picked);
  if (project.segmentId === row.suggestedProject?.segmentId) {
    next.delete(row.eventKey);
  } else {
    next.set(row.eventKey, project);
  }
  return next;
};

export const keepPendingPicks = (
  picked: PickedProjects,
  rows: EventMappingRow[],
): Map<string, EventMappingProject> => {
  const pendingKeys = new Set(rows.filter(isPending).map(({ eventKey }) => eventKey));
  return new Map([...picked].filter(([eventKey]) => pendingKeys.has(eventKey)));
};

export const keepSelectable = (
  selected: ReadonlySet<string>,
  rows: EventMappingRow[],
  picked: PickedProjects,
): Set<string> =>
  new Set(
    rows
      .filter((row) => selected.has(row.eventKey) && isSelectable(row, picked))
      .map(({ eventKey }) => eventKey),
  );

export const confirmItem = (
  row: EventMappingRow,
  picked: PickedProjects,
): EventMappingConfirmItem | null => {
  const project = displayedProject(row, picked);
  return isPending(row) && project
    ? { platform: row.platform, eventKey: row.eventKey, segmentId: project.segmentId }
    : null;
};

export const confirmItems = (
  rows: EventMappingRow[],
  selected: ReadonlySet<string>,
  picked: PickedProjects,
): EventMappingConfirmItem[] =>
  rows
    .filter((row) => selected.has(row.eventKey))
    .map((row) => confirmItem(row, picked))
    .filter((item): item is EventMappingConfirmItem => item !== null);

export const formatEventDates = (start: string, end: string): string => {
  const from = dateHelper(start);
  const to = dateHelper(end);
  if (from.isSame(to, 'day')) {
    return from.format('MMM D, YYYY');
  }
  if (from.isSame(to, 'month')) {
    return `${from.format('MMM D')}–${to.format('D, YYYY')}`;
  }
  if (from.isSame(to, 'year')) {
    return `${from.format('MMM D')} – ${to.format('MMM D, YYYY')}`;
  }
  return `${from.format('MMM D, YYYY')} – ${to.format('MMM D, YYYY')}`;
};

export const formatDecisionDate = (decidedAt: string): string =>
  dateHelper(decidedAt).format('MMM D, YYYY');

export const groupProjects = (subprojects: PickerSubproject[]): ProjectGroupOption[] => {
  const groups = new Map<string, EventMappingProject[]>();
  subprojects.forEach(({ id, name, grandparentName }) => {
    const project = { segmentId: id, name, groupName: grandparentName };
    groups.set(grandparentName, [...(groups.get(grandparentName) ?? []), project]);
  });
  return [...groups.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([groupName, projects]) => ({ groupName, projects }));
};
