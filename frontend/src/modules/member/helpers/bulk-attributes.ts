// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { dateHelper } from '@/shared/date-helper/date-helper';

interface AttributeValues {
  default?: unknown;
}

export interface BulkAttributesMember {
  id: string;
  joinedAt?: string | null;
  organizations: { id: string }[];
  attributes: Record<string, AttributeValues>;
}

export interface BulkAttributesToSave {
  joinedAt?: string;
  organizations?: { id: string }[];
  attributes?: Record<string, AttributeValues | undefined>;
}

export interface BulkUpdateMemberPayload {
  attributes?: Record<string, AttributeValues> | null;
  organizations?: string[];
  joinedAt?: string | null;
  id: string;
}

const mergeAttributes = (
  current: Record<string, AttributeValues>,
  toSave: Record<string, AttributeValues | undefined>,
): Record<string, AttributeValues> => {
  const merged = { ...current };
  Object.keys(toSave).forEach((attributeName) => {
    const attributeValue = toSave[attributeName];
    if (attributeValue && Array.isArray(attributeValue.default)) {
      const existing = merged[attributeName] || { default: [] };
      if (!Array.isArray(existing.default)) {
        throw new TypeError(`Attribute ${attributeName} has no list of values to append to`);
      }
      const existingValues: unknown[] = existing.default;
      const newValues = attributeValue.default.filter((value) => !existingValues.includes(value));
      merged[attributeName] = { ...existing, default: [...existingValues, ...newValues] };
    } else if (attributeValue && typeof attributeValue.default !== 'undefined') {
      merged[attributeName] = { default: attributeValue.default };
    }
  });
  return merged;
};

const castOrganizations = (organizations: { id: string }[] | null | undefined): string[] | undefined => {
  if (organizations === undefined) {
    return undefined;
  }
  if (!organizations || !organizations.length) {
    return [];
  }
  return organizations.map((organization) => organization.id);
};

const castJoinedAt = (joinedAt: string | null | undefined): string | null | undefined => {
  if (joinedAt === undefined) {
    return undefined;
  }
  return joinedAt ? dateHelper(joinedAt).toJSON() : null;
};

export const buildBulkUpdatePayload = (
  members: BulkAttributesMember[],
  attributesToSave: BulkAttributesToSave,
): BulkUpdateMemberPayload[] => members.map((member) => {
  const joinedAt = attributesToSave.joinedAt ? attributesToSave.joinedAt : member.joinedAt;

  let { organizations } = member;
  if (attributesToSave.organizations) {
    const orgIdsInMember = member.organizations.map((org) => org.id);
    organizations = [
      ...member.organizations,
      ...attributesToSave.organizations.filter((org) => !orgIdsInMember.includes(org.id)),
    ];
  }

  const attributes = attributesToSave.attributes
    ? mergeAttributes(member.attributes, attributesToSave.attributes)
    : member.attributes;

  return {
    attributes,
    organizations: castOrganizations(organizations),
    joinedAt: castJoinedAt(joinedAt),
    id: member.id,
  };
});
