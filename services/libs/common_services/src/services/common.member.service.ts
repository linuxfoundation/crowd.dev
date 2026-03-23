import isEqual from 'lodash.isequal'
import pick from 'lodash.pick'
import moment from 'moment'

import {
  captureApiChange,
  memberEditOrganizationsAction,
  memberMergeAction,
} from '@crowd/audit-logs'
import {
  DEFAULT_TENANT_ID,
  Error409,
  calculateReach,
  getEarliestValidDate,
  getLongestDateRange,
  mergeObjects,
  safeObjectMerge,
} from '@crowd/common'
import {
  MEMBER_MERGE_FIELDS,
  MemberField,
  QueryExecutor,
  changeMemberOrganizationAffiliationOverrides,
  checkOrganizationAffiliationPolicy,
  createOrUpdateMemberOrganizations,
  deleteMemberOrganizations,
  fetchManyMemberOrgsWithOrgData,
  fetchMemberOrganizations,
  findAllUnkownDatedOrganizations,
  findIdentitiesForMembers,
  findMemberById,
  findMemberCountEstimateOfOrganizations,
  findMemberManualAffiliation,
  findMemberWorkExperience,
  findMostRecentUnknownDatedOrganizations,
  getMemberSegments,
  includeMemberToSegments,
  moveAffiliationsBetweenMembers,
  moveIdentitiesBetweenMembers,
  moveOrgsBetweenMembers,
  updateMember,
} from '@crowd/data-access-layer'
import { removeMemberToMerge } from '@crowd/data-access-layer/src/member_merge'
import { findMemberAffiliations } from '@crowd/data-access-layer/src/member_segment_affiliations'
import {
  addMergeAction,
  queryMergeActions,
  setMergeAction,
} from '@crowd/data-access-layer/src/mergeActions/repo'
import { IWorkExperienceData } from '@crowd/data-access-layer/src/old/apps/data_sink_worker/repo/memberAffiliation.data'
import { addOrgsToSegments } from '@crowd/data-access-layer/src/organizations'
import { Logger, LoggerBase } from '@crowd/logging'
import { Client as TemporalClient, WorkflowIdReusePolicy } from '@crowd/temporal'
import {
  MergeActionState,
  MergeActionStep,
  MergeActionType,
  TemporalWorkflowId,
} from '@crowd/types'

/* eslint-disable @typescript-eslint/no-explicit-any */

export class CommonMemberService extends LoggerBase {
  public constructor(
    private readonly qx: QueryExecutor,
    private readonly temporal: TemporalClient,
    parentLog: Logger,
  ) {
    super(parentLog)
  }

  public async updateMemberOrganizations(
    memberId: string,
    organizations: any,
    replace: any,
    segmentIds: string[],
    options?: any,
  ): Promise<void> {
    if (!organizations) {
      return
    }

    function iso(v) {
      return moment(v).toISOString()
    }

    await captureApiChange(
      options,
      memberEditOrganizationsAction(memberId, async (captureOldState, captureNewState) => {
        const originalOrgs = await fetchMemberOrganizations(this.qx, memberId)

        captureOldState(originalOrgs)
        const newOrgs = [...originalOrgs]

        if (replace) {
          const toDelete = originalOrgs.filter(
            (originalOrg: any) =>
              !organizations.find(
                (newOrg) =>
                  originalOrg.organizationId === newOrg.id &&
                  (originalOrg.title === (newOrg.title || null) ||
                    (!originalOrg.title && newOrg.title)) &&
                  iso(originalOrg.dateStart) === iso(newOrg.startDate || null) &&
                  iso(originalOrg.dateEnd) === iso(newOrg.endDate || null),
              ),
          )

          for (const item of toDelete) {
            await deleteMemberOrganizations(this.qx, memberId, [item.id])
            ;(item as any).delete = true
          }
        }

        for (const item of organizations) {
          const org = typeof item === 'string' ? { id: item } : item

          // we don't need to touch exactly same existing work experiences
          if (
            !originalOrgs.some(
              (w) =>
                w.organizationId === item.id &&
                w.title === (item.title || null) &&
                w.dateStart === (item.startDate || null) &&
                w.dateEnd === (item.endDate || null),
            )
          ) {
            const newOrg = {
              memberId,
              organizationId: org.id,
              title: org.title,
              dateStart: org.startDate,
              dateEnd: org.endDate,
              source: org.source,
            }

            const newMemberOrgId = await createOrUpdateMemberOrganizations(
              this.qx,
              memberId,
              org.id,
              org.source,
              org.title,
              org.startDate,
              org.endDate,
            )

            const isAffiliationBlocked = await checkOrganizationAffiliationPolicy(this.qx, org.id)

            if (newMemberOrgId && isAffiliationBlocked) {
              await changeMemberOrganizationAffiliationOverrides(this.qx, [
                {
                  memberId,
                  memberOrganizationId: newMemberOrgId,
                  allowAffiliation: false,
                },
              ])
            }

            await addOrgsToSegments(this.qx, segmentIds, [org.id])
            newOrgs.push(newOrg)
          }
        }

        captureNewState(newOrgs)
      }),
    )
  }

  public async findAffiliation(
    memberId: string,
    segmentId: string,
    timestamp: string,
  ): Promise<string | null> {
    const manualAffiliation = await findMemberManualAffiliation(
      this.qx,
      memberId,
      segmentId,
      timestamp,
    )
    if (manualAffiliation) {
      return manualAffiliation.organizationId
    }

    const currentEmployments = await findMemberWorkExperience(this.qx, memberId, timestamp)
    if (currentEmployments.length > 0) {
      return this.decidePrimaryOrganizationId(currentEmployments)
    }

    const mostRecentUnknownDatedOrgs = await findMostRecentUnknownDatedOrganizations(
      this.qx,
      memberId,
      timestamp,
    )
    if (mostRecentUnknownDatedOrgs.length > 0) {
      return this.decidePrimaryOrganizationId(mostRecentUnknownDatedOrgs)
    }

    const allUnkownDAtedOrgs = await findAllUnkownDatedOrganizations(this.qx, memberId)
    if (allUnkownDAtedOrgs.length > 0) {
      return this.decidePrimaryOrganizationId(allUnkownDAtedOrgs)
    }

    return null
  }

  public async decidePrimaryOrganizationId(
    experiences: IWorkExperienceData[],
  ): Promise<string | null> {
    if (experiences.length > 0) {
      if (experiences.length === 1) {
        return experiences[0].organizationId
      }

      // check if any of the employements are marked as primary
      const primaryEmployment = experiences.find((employment) => employment.isPrimaryWorkExperience)

      if (primaryEmployment) {
        return primaryEmployment.organizationId
      }

      // decide based on the member count in the organizations
      const memberCounts = await findMemberCountEstimateOfOrganizations(
        this.qx,
        experiences.map((e) => e.organizationId),
      )

      if (memberCounts[0].memberCount > memberCounts[1].memberCount) {
        return memberCounts[0].organizationId
      } else if (memberCounts[0].memberCount < memberCounts[1].memberCount) {
        return memberCounts[1].organizationId
      }

      // if there's a draw in the member count, use the one with the longer period
      return getLongestDateRange(experiences).organizationId
    }

    return null
  }

  public async startAffiliationRecalculation(
    memberId: string,
    organizationIds: string[],
    syncToOpensearch = false,
  ): Promise<void> {
    await this.temporal.workflow.start('memberUpdate', {
      taskQueue: 'profiles',
      workflowId: `${TemporalWorkflowId.MEMBER_UPDATE}/${DEFAULT_TENANT_ID}/${memberId}`,
      workflowIdReusePolicy: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
      retry: {
        maximumAttempts: 10,
      },
      args: [
        {
          member: {
            id: memberId,
          },
          memberOrganizationIds: organizationIds,
          syncToOpensearch,
        },
      ],
      searchAttributes: {
        TenantId: [DEFAULT_TENANT_ID],
      },
    })
  }

  public async merge(
    originalId: string,
    toMergeId: string,
    options?: any,
  ): Promise<{ status: number; mergedId: string }> {
    this.log.info({ originalId, toMergeId }, 'Merging members!')

    if (originalId === toMergeId) {
      return {
        status: 203,
        mergedId: originalId,
      }
    }

    const mergeActions = await queryMergeActions(this.qx, {
      fields: ['id', 'state'],
      filter: {
        and: [
          {
            state: {
              eq: MergeActionState.IN_PROGRESS,
            },
          },
          {
            or: [
              { primaryId: { eq: originalId } },
              { secondaryId: { eq: originalId } },
              { primaryId: { eq: toMergeId } },
              { secondaryId: { eq: toMergeId } },
            ],
          },
        ],
      },
      limit: 1,
      orderBy: '"updatedAt" DESC',
    })

    // prevent multiple merge operations
    if (mergeActions.length > 0) {
      throw new Error409(options?.language, 'merge.errors.multiple', mergeActions[0].state)
    }

    try {
      const { original, toMerge } = await captureApiChange(
        options,
        memberMergeAction(originalId, async (captureOldState, captureNewState) => {
          const original = await this.getMemberById(originalId)
          const toMerge = await this.getMemberById(toMergeId)

          captureOldState({
            primary: original,
            secondary: toMerge,
          })

          const allIdentities = await findIdentitiesForMembers(this.qx, [originalId, toMergeId])

          const originalIdentities = allIdentities.get(originalId)
          const toMergeIdentities = allIdentities.get(toMergeId)

          const memberOrganizationMap = await fetchManyMemberOrgsWithOrgData(this.qx, [
            originalId,
            toMergeId,
          ])

          const backup = {
            primary: {
              ...pick(original, MEMBER_MERGE_FIELDS),
              identities: originalIdentities,
              memberOrganizations: memberOrganizationMap.get(originalId),
            },
            secondary: {
              ...pick(toMerge, MEMBER_MERGE_FIELDS),
              identities: toMergeIdentities,
              memberOrganizations: memberOrganizationMap.get(toMergeId),
            },
          }

          await addMergeAction(
            this.qx,
            MergeActionType.MEMBER,
            originalId,
            toMergeId,
            MergeActionStep.MERGE_STARTED,
            MergeActionState.IN_PROGRESS,
            backup,
            options?.currentUser?.id,
          )

          await this.qx.tx(async (txQx) => {
            const identitiesToUpdate = []
            const identitiesToMove = []
            for (const identity of toMergeIdentities) {
              const existing = originalIdentities.find(
                (i) =>
                  i.platform === identity.platform &&
                  i.type === identity.type &&
                  i.value === identity.value,
              )

              if (existing) {
                // if it's not verified but it should be
                if (!existing.verified && identity.verified) {
                  identitiesToUpdate.push(identity)
                }
              } else {
                identitiesToMove.push(identity)
              }
            }

            await moveIdentitiesBetweenMembers(
              txQx,
              toMergeId,
              originalId,
              identitiesToMove,
              identitiesToUpdate,
            )

            // Update member segment affiliations and organization affiliation overrides
            await moveAffiliationsBetweenMembers(txQx, toMergeId, originalId)

            // Performs a merge and returns the fields that were changed so we can update
            const toUpdate: any = await CommonMemberService.membersMerge(original, toMerge)

            captureNewState({ primary: toUpdate })

            // Update original member
            await updateMember(txQx, originalId, toUpdate)

            // update members that belong to source organization to destination org
            await moveOrgsBetweenMembers(txQx, originalId, toMergeId)

            // Remove toMerge from original member
            await removeMemberToMerge(txQx, originalId, toMergeId)

            const secondMemberSegments = await getMemberSegments(txQx, toMergeId)

            await includeMemberToSegments(
              txQx,
              toMergeId,
              secondMemberSegments.map((s) => s.id),
            )
          })

          this.log.info({ originalId, toMergeId }, '[Merge Members] - Transaction commited! ')

          await setMergeAction(this.qx, MergeActionType.MEMBER, originalId, toMergeId, {
            step: MergeActionStep.MERGE_SYNC_DONE,
          })

          return { original, toMerge }
        }),
      )

      await this.temporal.workflow.start('finishMemberMerging', {
        taskQueue: 'entity-merging',
        workflowId: `finishMemberMerging/${originalId}/${toMergeId}`,
        retry: {
          maximumAttempts: 10,
        },
        args: [
          originalId,
          toMergeId,
          original.displayName,
          toMerge.displayName,
          options?.currentUser?.id,
        ],
        searchAttributes: {
          TenantId: [DEFAULT_TENANT_ID],
        },
      })

      this.log.info({ originalId, toMergeId }, 'Members merged!')
      return { status: 200, mergedId: originalId }
    } catch (err) {
      if (err.name === 'WorkflowExecutionAlreadyStartedError') {
        this.log.info({ originalId, toMergeId }, 'Temporal workflow already started!')
        return { status: 409, mergedId: originalId }
      }

      this.log.error(err, 'Error while merging members!', { originalId, toMergeId })

      await setMergeAction(this.qx, MergeActionType.MEMBER, originalId, toMergeId, {
        state: MergeActionState.ERROR,
      })

      throw err
    }
  }

  private async getMemberById(memberId: string) {
    const member = await findMemberById(this.qx, memberId, [
      MemberField.ID,
      MemberField.DISPLAY_NAME,
      MemberField.JOINED_AT,
      MemberField.TENANT_ID,
      MemberField.REACH,
      MemberField.SCORE,
      MemberField.CONTRIBUTIONS,
      MemberField.ATTRIBUTES,
      MemberField.MANUALLY_CREATED,
      MemberField.MANUALLY_CHANGED_FIELDS,
    ])

    const affiliations = await findMemberAffiliations(this.qx, memberId)

    return {
      ...member,
      affiliations,
    }
  }

  /**
   * Call the merge function with the special fields for members.
   * We want to always keep the earlies joinedAt date.
   * We always want the original displayName.
   * @param originalObject Original object to merge
   * @param toMergeObject Object to merge into the original object
   * @returns The updates to be performed on the original object
   */
  public static membersMerge(originalObject, toMergeObject) {
    return mergeObjects(originalObject, toMergeObject, {
      joinedAt: (oldDate, newDate) => getEarliestValidDate(oldDate, newDate),
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      displayName: (oldValue, _newValue) => oldValue,
      reach: (oldReach, newReach) => calculateReach(oldReach, newReach),
      score: (oldScore, newScore) => Math.max(oldScore, newScore),
      attributes: (oldAttributes, newAttributes) => safeObjectMerge(oldAttributes, newAttributes),
    })
  }

  isEqual = {
    displayName: (a, b) => a === b,
    attributes: (a, b) => isEqual(a, b),
    contributions: (a, b) => isEqual(a, b),
    score: (a, b) => a === b,
    reach: (a, b) => isEqual(a, b),
    importHash: (a, b) => a === b,
  }
}
