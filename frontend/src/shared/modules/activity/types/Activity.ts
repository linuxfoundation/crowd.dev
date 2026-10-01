import type { Member } from '@/modules/member/types/Member';
import type { Organization } from '@/modules/organization/types/Organization';
import type { Platform } from '../../platform/types/Platform';

export interface Activity {
  id: string;
  type: string;
  timestamp: string;
  platform: Platform;
  score: number;
  sourceId: string;
  sourceParentId: string;
  username: string;
  attributes: any;
  channel: string;
  body: string;
  title: string;
  url: string;
  sentiment: {
    label: string;
    mixed: number;
    neutral: number;
    negative: number;
    positive: number;
    sentiment: number;
  };
  organizationId: string;
  createdAt: string;
  updatedAt: string;
  deletedAt: string;
  memberId: string;
  segmentId: string;
  objectMemberId: string;
  conversationId: string;
  parentId: string;
  tenantId: string;
  createdById: string;
  updatedById: string;
  member: Member;
  parent: Activity;
  organization: Organization;
  display: {
    default: string;
    short: string;
    author: string;
    channel: string;
  };
}
