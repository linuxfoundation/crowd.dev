// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import type { User } from '@auth0/auth0-spa-js';

export type Auth0UserWithLfClaims = User & Record<string, string | undefined>;
