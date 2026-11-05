// Copyright (c) 2025 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import type { Member } from '@/modules/member/types/Member';

export interface MemberMergeSuggestion {
  similarity: number;
  members: Member[];
}

export interface MemberMergeSuggestionsQuery {
  similarity?: string[];
  displayName?: string;
  projectIds?: string[];
  subprojectIds?: string[];
}

export type MemberBotSuggestionMember = Pick<
  Member,
  'id' | 'displayName' | 'attributes' | 'activityCount'
>;

export interface MemberBotSuggestion {
  memberId: string;
  confidence: number;
  displayName: string;
  attributes: Member['attributes'];
  activityCount: string;
  member: MemberBotSuggestionMember;
}

export interface MemberMergeSuggestionRef {
  similarity: number;
  members: Pick<Member, 'id' | 'displayName'>[];
}

export type MemberBotSuggestionRef = Pick<MemberBotSuggestion, 'memberId' | 'confidence'>;
