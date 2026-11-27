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
  EventMappingConfirmation,
  EventMappingConfirmItem,
  EventMappingList,
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

export const confirmation = (
  row: EventMappingRow,
  picked: PickedProjects,
): EventMappingConfirmation | null => {
  const project = displayedProject(row, picked);
  return isPending(row) && project
    ? { platform: row.platform, eventKey: row.eventKey, project }
    : null;
};

export const confirmations = (
  rows: EventMappingRow[],
  selected: ReadonlySet<string>,
  picked: PickedProjects,
): EventMappingConfirmation[] =>
  rows
    .filter((row) => selected.has(row.eventKey))
    .map((row) => confirmation(row, picked))
    .filter((item): item is EventMappingConfirmation => item !== null);

export const toConfirmItems = (decisions: EventMappingConfirmation[]): EventMappingConfirmItem[] =>
  decisions.map(({ platform, eventKey, project }) => ({
    platform,
    eventKey,
    segmentId: project.segmentId,
  }));

const withRowChanges = (
  list: EventMappingList,
  changeOf: (row: EventMappingRow) => Partial<EventMappingRow> | null,
): EventMappingList => {
  const rows = list.rows.map((row) => {
    const change = changeOf(row);
    return change ? { ...row, ...change } : row;
  });
  return { rows, counts: statusCounts(rows, 'all') };
};

export const applyConfirmed = (
  list: EventMappingList,
  decisions: EventMappingConfirmation[],
  decidedAt: string,
): EventMappingList => {
  const projects = new Map(decisions.map(({ eventKey, project }) => [eventKey, project]));
  return withRowChanges(list, (row) => {
    const project = projects.get(row.eventKey);
    return project ? { state: 'confirmed', project, decidedAt, canUndo: true } : null;
  });
};

export const applyIgnored = (
  list: EventMappingList,
  eventKey: string,
  decidedAt: string,
): EventMappingList =>
  withRowChanges(list, (row) =>
    row.eventKey === eventKey
      ? { state: 'ignored', project: null, decidedAt, canUndo: true }
      : null,
  );

const restoredState = ({ suggestedProject }: EventMappingRow): EventMappingState =>
  suggestedProject ? 'suggested' : 'unresolved';

export const applyRestored = (list: EventMappingList, eventKey: string): EventMappingList =>
  withRowChanges(list, (row) => {
    if (row.eventKey !== eventKey) {
      return null;
    }
    return { state: restoredState(row), project: null, decidedAt: null, canUndo: false };
  });

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
