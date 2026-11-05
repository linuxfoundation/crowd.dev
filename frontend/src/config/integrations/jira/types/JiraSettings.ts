// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface JiraSettings {
  url: string;
  auth: {
    personalAccessToken: string;
    username: string;
    apiToken: string;
  };
  projects?: string[];
}
