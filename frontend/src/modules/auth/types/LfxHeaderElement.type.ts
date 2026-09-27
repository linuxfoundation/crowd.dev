// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import type { Auth0UserWithLfClaims } from '@/modules/auth/types/Auth0UserWithLfClaims.type';

export interface LfxHeaderElement extends HTMLElement {
  authuser?: Auth0UserWithLfClaims;
  docslink?: string;
  supportlink?: string;
}
