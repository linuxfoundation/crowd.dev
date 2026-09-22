import { DEFAULT_TENANT_ID } from '@crowd/common'
import {
  changeMemberOrganizationAffiliationOverrides,
  fetchManyOrganizationAffiliationPolicies,
} from '@crowd/data-access-layer'
import { DbStore } from '@crowd/data-access-layer/src/database'
import { deleteMemberSegmentAffiliations } from '@crowd/data-access-layer/src/member_segment_affiliations'
import {
  IFindOrCreateOrganizationResult,
  addOrgsToMember,
  addOrgsToSegments,
  findMemberOrganizations,
  findOrCreateOrganization,
} from '@crowd/data-access-layer/src/organizations'
import { dbStoreQx } from '@crowd/data-access-layer/src/queryExecutor'
import { Logger, LoggerBase } from '@crowd/logging'
import { Client as TemporalClient } from '@crowd/temporal'
import {
  IMemberOrganization,
  IOrganization,
  IOrganizationIdSource,
  TemporalWorkflowId,
} from '@crowd/types'

export class OrganizationService extends LoggerBase {
  constructor(
    private readonly store: DbStore,
    private readonly temporal: TemporalClient,
    parentLog: Logger,
  ) {
    super(parentLog)
  }

  public async findOrCreate(
    source: string,
    integrationId: string,
    data: IOrganization,
  ): Promise<IFindOrCreateOrganizationResult | undefined> {
    return this.store.transactionally(async (txStore) => {
      const qe = dbStoreQx(txStore)
      return findOrCreateOrganization(qe, source, data, integrationId, true)
    })
  }

  public async addToMember(
    segmentIds: string[],
    memberId: string,
    orgs: IOrganizationIdSource[],
  ): Promise<void> {
    const qe = dbStoreQx(this.store)

    await addOrgsToSegments(
      qe,
      segmentIds,
      orgs.map((org) => org.id),
    )

    const newMemberOrgs = await addOrgsToMember(qe, memberId, orgs)

    const orgAffiliationPolicies = await fetchManyOrganizationAffiliationPolicies(
      qe,
      newMemberOrgs.map((mo) => mo.organizationId),
    )

    const memberOrgsWithBlockedAffiliations = newMemberOrgs.filter((mo) =>
      orgAffiliationPolicies.get(mo.organizationId),
    )

    const overrides = memberOrgsWithBlockedAffiliations.map((mo) => ({
      memberId,
      memberOrganizationId: mo.memberOrganizationId,
      allowAffiliation: false,
    }))

    if (overrides.length > 0) {
      await changeMemberOrganizationAffiliationOverrides(qe, overrides)
    }

    for (const organizationId of new Set(
      memberOrgsWithBlockedAffiliations.map((mo) => mo.organizationId),
    )) {
      await deleteMemberSegmentAffiliations(qe, { memberId, organizationId })
    }
  }

  public async findMemberOrganizations(
    memberId: string,
    organizationId: string,
  ): Promise<IMemberOrganization[]> {
    const qe = dbStoreQx(this.store)

    return findMemberOrganizations(qe, memberId, organizationId)
  }

  public async startFakeOrganizationAnalysisWorkflow(organizationId: string): Promise<void> {
    try {
      await this.temporal.workflow.start('fakeOrganizationAnalysisWithLLM', {
        taskQueue: 'profiles',
        workflowId: `${TemporalWorkflowId.FAKE_ORGANIZATION_ANALYSIS_WITH_LLM}/${organizationId}`,
        retry: {
          maximumAttempts: 10,
        },
        args: [{ organizationId }],
        searchAttributes: {
          TenantId: [DEFAULT_TENANT_ID],
        },
      })
    } catch (err) {
      if (err.name === 'WorkflowExecutionAlreadyStartedError') {
        this.log.info(
          { organizationId },
          'Fake organization analysis workflow already started, skipping',
        )
        return
      }

      throw err
    }
  }
}
