// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { dateHelper } from '@/shared/date-helper/date-helper';
import {
  manualPickBadge,
  manualPickEvidence,
  matchBadge,
  matchEvidence,
} from '@/modules/admin/modules/event-mapping/config/match-labels';
import type {
  DecidedNow,
  EventMappingConfirmItem,
  EventMappingListState,
  EventMappingPlatformFilter,
  EventMappingProject,
  EventMappingRow,
  EventMappingState,
  MatchBadge,
  PickedProjects,
  ProjectGroupOption,
  StatusCounts,
} from '@/modules/admin/modules/event-mapping/types/EventMapping';

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
  if (row.state === 'confirmed') {
    return row.project;
  }
  return picked.get(row.eventKey) ?? row.suggestedProject;
};

const isManualPick = (row: EventMappingRow, picked: PickedProjects): boolean => {
  if (row.state === 'confirmed') {
    return row.project?.segmentId !== row.suggestedProject?.segmentId;
  }
  return picked.has(row.eventKey);
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

export const keepKnownPicks = (
  picked: PickedProjects,
  rows: EventMappingRow[],
): Map<string, EventMappingProject> => {
  const knownKeys = new Set(rows.map(({ eventKey }) => eventKey));
  return new Map([...picked].filter(([eventKey]) => knownKeys.has(eventKey)));
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

export const decisionNote = (row: EventMappingRow, decidedNow: DecidedNow): string => {
  if (decidedNow.has(row.eventKey)) {
    return 'Just now';
  }
  return row.decidedAt ? dateHelper(row.decidedAt).format('MMM D, YYYY') : '';
};

export const keepDecidedNow = (
  decidedNow: DecidedNow,
  rows: EventMappingRow[],
): Map<string, string | null> => {
  const rowsByKey = new Map(rows.map((row) => [row.eventKey, row]));
  const kept = new Map<string, string | null>();
  decidedNow.forEach((recordedAt, eventKey) => {
    const row = rowsByKey.get(eventKey);
    if (!row || isPending(row) || (recordedAt !== null && row.decidedAt !== recordedAt)) {
      return;
    }
    kept.set(eventKey, row.decidedAt);
  });
  return kept;
};

export const groupProjects = (projects: EventMappingProject[]): ProjectGroupOption[] =>
  projects.reduce<ProjectGroupOption[]>((groups, project) => {
    const last = groups[groups.length - 1];
    if (last?.groupName === project.groupName) {
      last.projects.push(project);
      return groups;
    }
    return [...groups, { groupName: project.groupName, projects: [project] }];
  }, []);

export const listState = ({
  hasData,
  hasError,
}: {
  hasData: boolean;
  hasError: boolean;
}): EventMappingListState => {
  if (hasError) {
    return hasData ? 'refresh-failed' : 'load-failed';
  }
  return hasData ? 'ready' : 'loading';
};
