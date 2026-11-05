// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface IntegrationApiError {
  // shortcut: response typed as always present. revisit: D-15 types the API client errors
  response: { data: string };
}
