// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { LocationQueryValue } from 'vue-router';
import type { IntegrationApiError } from '@/modules/integration/types/IntegrationApiError';

type QueryParam = LocationQueryValue | LocationQueryValue[];

interface SegmentTarget {
  segmentId: string | null;
  grandparentId: string | null;
}

export interface GithubConnectPayload {
  code: QueryParam;
  installId: QueryParam;
  setupAction: QueryParam;
}

export interface GitlabConnectPayload extends SegmentTarget {
  code: QueryParam;
  state: QueryParam;
}

export interface GitConnectPayload extends SegmentTarget {
  remotes: string[];
  isUpdate?: unknown;
  errorHandler?: (error: IntegrationApiError) => void;
}

export interface GerritConnectPayload extends SegmentTarget {
  orgURL: string;
  repoNames: string[];
  enableAllRepos: boolean;
  enableGit: boolean;
  isUpdate?: unknown;
  errorHandler?: (error: IntegrationApiError) => void;
}

export interface ConfluenceConnectPayload extends SegmentTarget {
  id?: string;
  settings: Record<string, unknown>;
  isUpdate?: unknown;
}

export interface DiscourseConnectPayload extends SegmentTarget {
  forumHostname: string;
  apiKey: string;
  webhookSecret?: string;
  isUpdate?: unknown;
}

export interface GroupsioConnectPayload extends SegmentTarget {
  email: string;
  token: string;
  tokenExpiry: string;
  password: string;
  groups: unknown[];
  autoImports: unknown[];
  isUpdate?: unknown;
}

export interface JiraConnectPayload extends SegmentTarget {
  id?: string;
  url: string;
  username: string;
  personalAccessToken: string;
  apiToken: string;
  projects: string[];
  isUpdate?: unknown;
}

export interface RedditOnboardPayload extends SegmentTarget {
  subreddits: string[];
}

export type LinkedinConnectPayload = SegmentTarget;

export interface LinkedinOnboardPayload extends SegmentTarget {
  organizationId: string | null;
}

export interface DiscordConnectPayload {
  guildId: QueryParam;
}

export interface DevtoConnectPayload extends SegmentTarget {
  users: string[];
  organizations: string[];
  apiKey: string;
}

export interface HackerNewsConnectPayload extends SegmentTarget {
  keywords: string[];
  urls: string[];
}

export interface StackOverflowOnboardPayload extends SegmentTarget {
  tags: string[];
  keywords: string[];
}
