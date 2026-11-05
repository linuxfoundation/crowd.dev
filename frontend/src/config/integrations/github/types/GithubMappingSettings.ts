// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { Integration } from '@/modules/admin/modules/integration/types/Integration';

export interface GithubMappingRepo {
  name: string;
  url: string;
}

export interface GithubMappingSettings {
  orgs: {
    name: string;
    logo: string;
    repos: GithubMappingRepo[];
  }[];
}

export interface GithubMappingIntegration extends Integration<GithubMappingSettings> {
  segmentId: string;
}
