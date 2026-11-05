// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface DataQualityMemberRef {
  id: string;
  displayName: string;
  avatarUrl?: string | null;
}

export interface DataQualityOrganizationRef {
  id: string;
  displayName: string;
  logo?: string | null;
}

export interface DataQualityMemberMergeSuggestion {
  id?: string;
  similarity: number;
  members: DataQualityMemberRef[];
}

export interface DataQualityOrganizationMergeSuggestion {
  id?: string;
  similarity: number;
  organizations: DataQualityOrganizationRef[];
}

export interface DataQualityMemberBotSuggestion {
  memberId: string;
  displayName: string;
  avatarUrl?: string | null;
  confidence: number;
  attributes: Record<string, unknown>;
}

export interface DataQualityFakeOrganizationSuggestion {
  organizationId: string;
  displayName: string;
  logo?: string | null;
  activityCount: number;
}

export interface DataQualityIssuesParams {
  type?: string;
  limit: number;
  offset: number;
}
