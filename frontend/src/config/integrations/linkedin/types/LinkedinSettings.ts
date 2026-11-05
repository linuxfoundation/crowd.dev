// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

export interface LinkedinOrganization {
  id: string;
  name: string;
  vanityName?: string;
  inUse: boolean;
}

export interface LinkedinSettings {
  organizations: LinkedinOrganization[];
}
