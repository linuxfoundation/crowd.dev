// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
export interface Auth0UserWithLfClaims {
  name?: string;
  email?: string;
  [claim: string]: unknown;
}
