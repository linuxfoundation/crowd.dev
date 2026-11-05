// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface ProfileMergeSocketPayload {
  success: boolean;
  primaryDisplayName: string;
  secondaryDisplayName: string;
  primaryId: string;
  secondaryId: string;
  userId: string;
}

export interface OrganizationMergeSocketPayload {
  success: boolean;
  userId: string;
  primaryOrgId: string;
  original: string;
  toMerge: string;
}

export interface IntegrationCompletedSocketPayload {
  integrationId: string;
  status: string;
}

export interface ServerToClientEvents {
  connect: () => void;
  disconnect: () => void;
  'integration-completed': (payload: string) => void;
  'member-merge': (payload: string) => void;
  'member-unmerge': (payload: string) => void;
  'organization-unmerge': (payload: string) => void;
  'org-merge': (payload: string) => void;
}
