// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { Integration } from '@/modules/admin/modules/integration/types/Integration';

export interface GitlabProject {
  id: number;
  path_with_namespace: string;
  groupName?: string;
  url: string;
}

export interface GitlabMappedProject extends GitlabProject {
  web_url: string;
  name?: string;
}

export interface GitlabSettings {
  user: { name: string; username: string; avatar_url?: string };
  groups: { id: number; name: string; avatarUrl?: string }[];
  userProjects: GitlabProject[];
  groupProjects: Record<string, GitlabProject[]>;
}

export interface GitlabIntegration extends Integration<GitlabSettings> {
  segmentId: string;
}
