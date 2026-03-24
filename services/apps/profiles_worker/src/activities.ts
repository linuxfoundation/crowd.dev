// Leaf segment aggregate calculation
import {
  calculateAllMemberLeafAggregates,
  calculateAllOrganizationLeafAggregates,
} from './activities/calculateLeafSegmentAggregates'
import { getLLMResult } from './activities/common'
import {
  createMemberBotSuggestion,
  createMemberNoBot,
  getMemberForBotAnalysis,
  removeMemberOrganizations,
  updateMemberAttributes,
} from './activities/member/botSuggestion'
import {
  calculateProjectGroupMemberAggregates,
  calculateProjectMemberAggregates,
  getSegmentHierarchy,
} from './activities/member/memberAggregates'
import {
  getMemberDisplayAggregates,
  getMemberDisplayAggsLastSyncedAt,
  setMemberDisplayAggregates,
  touchMemberDisplayAggsLastSyncedAt,
} from './activities/member/memberDisplayAggs'
import { syncMember, updateMemberAffiliations } from './activities/member/memberUpdate'
import {
  calculateProjectGroupOrganizationAggregates,
  calculateProjectOrganizationAggregates,
} from './activities/organization/organizationAggregates'
import {
  getOrganizationDisplayAggregates,
  getOrganizationDisplayAggsLastSyncedAt,
  setOrganizationDisplayAggregates,
  touchOrganizationDisplayAggsLastSyncedAt,
} from './activities/organization/organizationDisplayAggs'
import {
  findMembersInOrganization,
  syncOrganization,
} from './activities/organization/organizationUpdate'

export {
  updateMemberAffiliations,
  syncMember,
  syncOrganization,
  findMembersInOrganization,
  // Member aggregates
  getSegmentHierarchy,
  calculateProjectMemberAggregates,
  calculateProjectGroupMemberAggregates,
  // Organization aggregates
  calculateProjectOrganizationAggregates,
  calculateProjectGroupOrganizationAggregates,
  // Legacy display aggs (can be removed once migration is complete)
  getMemberDisplayAggsLastSyncedAt,
  touchMemberDisplayAggsLastSyncedAt,
  getMemberDisplayAggregates,
  setMemberDisplayAggregates,
  getOrganizationDisplayAggsLastSyncedAt,
  touchOrganizationDisplayAggsLastSyncedAt,
  getOrganizationDisplayAggregates,
  setOrganizationDisplayAggregates,
  getMemberForBotAnalysis,
  updateMemberAttributes,
  removeMemberOrganizations,
  createMemberBotSuggestion,
  createMemberNoBot,
  getLLMResult,
  // Leaf segment aggregate calculation (scheduled every 5 minutes)
  calculateAllMemberLeafAggregates,
  calculateAllOrganizationLeafAggregates,
}
