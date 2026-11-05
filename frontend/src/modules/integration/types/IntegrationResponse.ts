// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { Integration } from '@/modules/admin/modules/integration/types/Integration';

export interface IntegrationRecord extends Integration<Record<string, unknown>> {
  platform: string;
  segmentId?: string;
  updatedAt?: string;
  lastProcessedAt?: string;
  loading?: boolean;
  isNango?: boolean;
}

export interface IntegrationResponse extends IntegrationRecord {
  segmentId: string;
}

export interface IntegrationWithSegment extends IntegrationResponse {
  segment: {
    id: string;
    grandparentId: string;
    name?: string;
  };
}

export interface MappedRepos {
  projects: {
    segmentName: string;
    url: string;
  }[];
  repositories: {
    url: string;
  }[];
}

export interface GithubInstallation {
  id: string;
  installationId: string;
  type: string;
  numRepos: number;
  login: string;
  avatarUrl: string;
}
