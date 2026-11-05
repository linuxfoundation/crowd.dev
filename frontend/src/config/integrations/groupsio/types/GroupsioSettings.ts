// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface GroupsioSettings {
  email: string;
  token: string;
  groups: { id?: string; name?: string; slug?: string }[];
}
