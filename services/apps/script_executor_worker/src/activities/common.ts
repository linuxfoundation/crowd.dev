import axios from 'axios'

import { CommonMemberService, signalMemberUpdate } from '@crowd/common_services'
import { pgpQx } from '@crowd/data-access-layer'
import {
  IMemberIdentity,
  IMemberUnmergeBackup,
  IMemberUnmergePreviewResult,
  IUnmergeBackup,
  IUnmergePreviewResult,
} from '@crowd/types'

import { svc } from '../main'

export async function mergeMembers(
  primaryMemberId: string,
  secondaryMemberId: string,
): Promise<void> {
  const qx = pgpQx(svc.postgres.writer.connection())
  const memberService = new CommonMemberService(qx, svc.temporal, svc.log)

  try {
    await memberService.merge(primaryMemberId, secondaryMemberId, {
      currentUser: {
        id: process.env['CROWD_LF_AGENT_USER_ID'],
      },
    })
  } catch (error) {
    svc.log.error({ err: error }, 'Failed to merge members')
    throw error
  }
}

export async function unmergeMembers(
  primaryMemberId: string,
  backup: IUnmergeBackup<IMemberUnmergeBackup> | IUnmergePreviewResult<IMemberUnmergePreviewResult>,
): Promise<void> {
  const url = `${process.env['CROWD_API_SERVICE_URL']}/member/${primaryMemberId}/unmerge`
  const requestOptions = {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${process.env['CROWD_LF_AGENT_USER_TOKEN']}`,
      'Content-Type': 'application/json',
    },
    data: {
      ...backup,
    },
  }

  try {
    await axios(url, requestOptions)
  } catch (error) {
    svc.log.error({ err: error, status: error.response?.status }, 'Failed to unmerge member')
    throw error
  }
}

export async function unmergeMembersPreview(
  memberId: string,
  memberIdentity: IMemberIdentity,
): Promise<IUnmergePreviewResult<IMemberUnmergePreviewResult>> {
  const url = `${process.env['CROWD_API_SERVICE_URL']}/member/${memberId}/unmerge/preview`
  const requestOptions = {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${process.env['CROWD_LF_AGENT_USER_TOKEN']}`,
      'Content-Type': 'application/json',
    },
    data: {
      platform: memberIdentity.platform,
      value: memberIdentity.value,
      type: memberIdentity.type,
    },
  }

  try {
    const result = await axios(url, requestOptions)
    return result.data
  } catch (error) {
    svc.log.error(
      { err: error, status: error.response?.status },
      'Failed to unmerge member preview',
    )
    throw error
  }
}

export async function mergeOrganizations(
  primaryOrgId: string,
  secondaryOrgId: string,
  segmentId?: string,
): Promise<void> {
  const url = `${process.env['CROWD_API_SERVICE_URL']}/organization/${primaryOrgId}/merge`
  const requestOptions = {
    method: 'PUT',
    headers: {
      Authorization: `Bearer ${process.env['CROWD_LF_AGENT_USER_TOKEN']}`,
      'Content-Type': 'application/json',
    },
    data: {
      organizationToMerge: secondaryOrgId,
      segments: segmentId ? [segmentId] : [],
    },
  }

  try {
    await axios(url, requestOptions)
  } catch (error) {
    svc.log.error({ err: error, status: error.response?.status }, 'Failed to merge organization')
    throw error
  }
}

export async function waitForTemporalWorkflowExecutionFinish(workflowId: string): Promise<void> {
  const handle = svc.temporal.workflow.getHandle(workflowId)

  const timeoutDuration = 1000 * 60 * 10 // 10 minutes

  try {
    // Wait for the workflow to complete or the timeout to occur
    await Promise.race([handle.result(), timeout(timeoutDuration, workflowId)])
  } catch (err) {
    console.error('Failed to get workflow result:', err.message)
  }
}

export function timeout(ms: number, workflowId: string): Promise<void> {
  return new Promise((_, reject) => {
    setTimeout(() => {
      reject(new Error(`Timeout waiting for workflow ${workflowId} to finish`))
    }, ms)
  })
}

export async function getWorkflowsCount(workflowType: string, status: string): Promise<number> {
  try {
    let totalCount = 0

    const handle = svc.temporal.workflow.list({
      query: `WorkflowType = '${workflowType}' AND ExecutionStatus = '${status}'`,
      pageSize: 1000,
    })

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    for await (const _ of handle) {
      totalCount++
    }

    return totalCount
  } catch (error) {
    svc.log.error(error, 'Error getting workflows count!')
    throw error
  }
}

export async function triggerMemberAffiliationsRefresh(memberId: string): Promise<void> {
  await signalMemberUpdate(svc.temporal, memberId)
}
