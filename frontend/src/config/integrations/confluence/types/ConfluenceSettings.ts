// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface ConfluenceSettings {
  url: string;
  username?: string;
  apiToken?: string;
  orgAdminApiToken?: string;
  orgAdminId?: string;
  space?: { key: string };
  spaces?: string[];
}
