// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { PillType } from '@/ui-kit/pill/types/PillType';

export type EventMappingPlatform = 'sessionize';

export type EventMappingState = 'suggested' | 'unresolved' | 'confirmed' | 'ignored';

export type EventMappingSuggestionKind =
  'same_name' | 'similar_name' | 'name_contained' | 'sub_event' | 'project_alias';

export type EventMappingUnresolvedReason =
  'ambiguous' | 'no_cvent_event' | 'main_event_not_in_cdp' | 'not_checked' | 'project_not_in_cdp';

export type EventMappingMatch = EventMappingSuggestionKind | EventMappingUnresolvedReason;

export interface EventMappingProject {
  segmentId: string;
  name: string;
  groupName: string;
}

export interface EventMappingCventEvent {
  name: string;
  startDate: string;
  dayGap: number;
}

export interface EventMappingRow {
  platform: EventMappingPlatform;
  eventKey: string;
  eventName: string;
  eventStart: string;
  eventEnd: string;
  state: EventMappingState;
  match: EventMappingMatch;
  cventEvent: EventMappingCventEvent | null;
  suggestedProject: EventMappingProject | null;
  hintProject: EventMappingProject | null;
  project: EventMappingProject | null;
  decidedAt: string | null;
  canUndo: boolean;
}

export type StatusCounts = Record<EventMappingState, number>;

export interface EventMappingList {
  rows: EventMappingRow[];
  counts: StatusCounts;
}

export interface EventMappingReference {
  platform: EventMappingPlatform;
  eventKey: string;
}

export interface EventMappingConfirmItem extends EventMappingReference {
  segmentId: string;
}

export type EventMappingPlatformFilter = 'all' | EventMappingPlatform;

export type PickedProjects = ReadonlyMap<string, EventMappingProject>;

export type DecidedNow = ReadonlyMap<string, string | null>;

export interface EventMappingProjectsPage {
  rows: EventMappingProject[];
  total: number;
}

export interface ProjectGroupOption {
  groupName: string;
  projects: EventMappingProject[];
}

export interface MatchBadge {
  label: string;
  pillType: PillType;
}

export interface EventMappingPlatformConfig {
  label: string;
  dotClass: string;
}

export interface EventMappingStatusTab {
  state: EventMappingState;
  label: string;
}

export interface EventMappingPlatformOption extends Pick<EventMappingPlatformConfig, 'label'> {
  value: EventMappingPlatformFilter;
  dotClass?: EventMappingPlatformConfig['dotClass'];
}

export type EventMappingListState = 'loading' | 'ready' | 'load-failed' | 'refresh-failed';
