// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface GerritSettings {
  remote?: {
    orgURL: string;
    repoNames?: string[];
    enableAllRepos?: boolean;
  };
}
