// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import type { FilterCustomAttribute } from '@/shared/modules/filters/types/FilterCustomAttribute';

export const parseCustomAttributes = (
  attributes: FilterCustomAttribute[] | Record<string, FilterCustomAttribute>,
) =>
  Object.values(attributes)
    .filter((attribute) => attribute.canDelete && attribute.show)
    .reduce<Record<string, FilterCustomAttribute>>(
      (obj, attribute) => ({ ...obj, [attribute.name]: attribute }),
      {},
    );
