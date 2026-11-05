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
