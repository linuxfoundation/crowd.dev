// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

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

export interface EventMappingList {
  rows: EventMappingRow[];
  counts: Record<EventMappingState, number>;
}

export interface EventMappingReference {
  platform: EventMappingPlatform;
  eventKey: string;
}

export interface EventMappingConfirmItem extends EventMappingReference {
  segmentId: string;
}

export interface EventMappingConfirmation extends EventMappingReference {
  project: EventMappingProject;
}
