import { WorkflowIdReusePolicy } from '@temporalio/client'

import { getAttributeValue } from '@crowd/common'
import {
  IDbOrgAttribute,
  MemberField,
  OrganizationField,
  fetchMemberIdentities,
  fetchOrgIdentities,
  fetchOrganizationMemberIds,
  findMemberById,
  findMemberOrganizations,
  findOrgAttributes,
  findOrgById,
  insertFakeOrganizationSuggestions,
  pgpQx,
  updateOrganization,
} from '@crowd/data-access-layer'
import { applyOrganizationAffiliationPolicyToMembers } from '@crowd/data-access-layer/src/member-organization-affiliation'
import { deleteMemberSegmentAffiliations } from '@crowd/data-access-layer/src/member_segment_affiliations'
import {
  IAttributes,
  IMemberIdentity,
  IMemberOrganization,
  IOrganizationIdentity,
  MemberAttributeName,
  OrganizationSource,
  TemporalWorkflowId,
} from '@crowd/types'

import { svc } from '../../main'

const SKIPPED_MEMBER_ATTRIBUTE_NAMES = new Set<string>([
  MemberAttributeName.IS_BOT,
  MemberAttributeName.IS_TEAM_MEMBER,
  MemberAttributeName.IS_ORGANIZATION,
  MemberAttributeName.AVATAR_URL,
  MemberAttributeName.SOURCE_ID,
  MemberAttributeName.SAMPLE,
  MemberAttributeName.KARMA,
  MemberAttributeName.SYNC_REMOTE,
  MemberAttributeName.EMAILS,
  MemberAttributeName.NAME,
])

const ROOT_ORG_ATTRIBUTE_NAMES = new Set([
  'name',
  'displayName',
  'description',
  'headline',
  'industry',
  'location',
  'type',
  'size',
])

export async function getOrganizationForFakeAnalysis(
  organizationId: string,
): Promise<Record<string, unknown> | null> {
  const qx = pgpQx(svc.postgres.reader.connection())

  const org = await findOrgById(qx, organizationId, [
    OrganizationField.DISPLAY_NAME,
    OrganizationField.DESCRIPTION,
    OrganizationField.HEADLINE,
    OrganizationField.INDUSTRY,
    OrganizationField.LOCATION,
    OrganizationField.TYPE,
    OrganizationField.SIZE,
  ])

  if (!org) {
    return null
  }

  const [identities, orgAttributes, memberIds] = await Promise.all([
    fetchOrgIdentities(qx, organizationId),
    findOrgAttributes(qx, organizationId),
    fetchOrganizationMemberIds(qx, organizationId, 5),
  ])

  const members: Record<string, unknown>[] = []

  for (const memberId of memberIds) {
    const [member, memberIdentities, memberOrgs] = await Promise.all([
      findMemberById(qx, memberId, [MemberField.DISPLAY_NAME, MemberField.ATTRIBUTES]),
      fetchMemberIdentities(qx, memberId),
      findMemberOrganizations(qx, memberId, organizationId),
    ])

    if (!member) {
      continue
    }

    const payload: Record<string, unknown> = {
      role: toRolePayload(memberOrgs),
    }

    if (member.displayName) {
      payload.displayName = member.displayName
    }

    const compactIdentities = toIdentityPayloads(memberIdentities)
    if (compactIdentities.length > 0) {
      payload.identities = compactIdentities
    }

    const attributes = flattenMemberAttributes(member.attributes)
    if (attributes) {
      payload.attributes = attributes
    }

    members.push(payload)
  }

  const context: Record<string, unknown> = {
    members,
  }

  if (org.displayName) {
    context.displayName = org.displayName
  }
  if (org.description) {
    context.description = org.description
  }
  if (org.headline) {
    context.headline = org.headline
  }
  if (org.industry) {
    context.industry = org.industry
  }
  if (org.location) {
    context.location = org.location
  }
  if (org.type) {
    context.type = org.type
  }
  if (org.size) {
    context.size = org.size
  }

  const compactOrgIdentities = toIdentityPayloads(identities)
  if (compactOrgIdentities.length > 0) {
    context.identities = compactOrgIdentities
  }

  const attributes = flattenOrgAttributes(orgAttributes)
  if (attributes) {
    context.attributes = attributes
  }

  return context
}

export async function markOrganizationAsFake(organizationId: string): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())

  await updateOrganization(qx, organizationId, { isAffiliationBlocked: true })
  await applyOrganizationAffiliationPolicyToMembers(qx, organizationId, false)
  await deleteMemberSegmentAffiliations(qx, { organizationId })

  const workflowId = `${TemporalWorkflowId.ORGANIZATION_UPDATE}/${organizationId}`

  try {
    await svc.temporal.workflow.start('organizationUpdate', {
      taskQueue: 'profiles',
      workflowId,
      workflowIdReusePolicy: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
      retry: {
        maximumAttempts: 10,
      },
      args: [
        {
          organization: {
            id: organizationId,
          },
          recalculateAffiliations: true,
          syncOptions: {
            doSync: true,
          },
        },
      ],
    })
  } catch (err) {
    if (err.name === 'WorkflowExecutionAlreadyStartedError') {
      svc.log.info({ workflowId }, 'Organization update workflow already started, skipping')
      return
    }

    throw err
  }
}

export async function createFakeOrganizationSuggestion(organizationId: string): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())

  await insertFakeOrganizationSuggestions(qx, [organizationId])
}

function toIdentityPayloads(identities: Array<IMemberIdentity | IOrganizationIdentity>) {
  return [...identities]
    .sort((a, b) => Number(b.verified) - Number(a.verified))
    .slice(0, 30)
    .map((identity) => ({
      platform: identity.platform,
      type: identity.type,
      value: identity.value,
      verified: identity.verified,
    }))
}

function toRolePayload(memberOrgs: IMemberOrganization[]) {
  const roles = memberOrgs.filter((mo) => !mo.deletedAt)
  const memberOrg =
    roles.find((mo) => mo.source === OrganizationSource.EMAIL_DOMAIN) ?? roles[0] ?? null

  return {
    title: memberOrg?.title ?? null,
    dateStart: memberOrg?.dateStart ?? null,
    dateEnd: memberOrg?.dateEnd ?? null,
    source: memberOrg?.source ?? null,
  }
}

function flattenMemberAttributes(attributes?: IAttributes): Record<string, string> | undefined {
  if (!attributes) {
    return undefined
  }

  const flattened: Record<string, string> = {}

  for (const [name, value] of Object.entries(attributes)) {
    if (SKIPPED_MEMBER_ATTRIBUTE_NAMES.has(name)) {
      continue
    }

    const resolved = getAttributeValue(value)
    if (resolved) {
      flattened[name] = resolved
    }
  }

  return Object.keys(flattened).length > 0 ? flattened : undefined
}

function flattenOrgAttributes(attributes: IDbOrgAttribute[]): Record<string, string> | undefined {
  const flattened: Record<string, string> = {}

  for (const attribute of attributes) {
    if (!attribute.default || !attribute.value || ROOT_ORG_ATTRIBUTE_NAMES.has(attribute.name)) {
      continue
    }

    flattened[attribute.name] = attribute.value
  }

  return Object.keys(flattened).length > 0 ? flattened : undefined
}
