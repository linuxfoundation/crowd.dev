import { generateUUIDv4 } from '@crowd/common'
import { ITriggerCSVExport, TemporalWorkflowId } from '@crowd/types'

import Permissions from '../../security/permissions'
import identifyTenant from '../../segment/identifyTenant'
import track from '../../segment/track'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * POST /member/export
 * @summary Export members as CSV
 * @tag Members
 * @security Bearer
 * @description Export members. It accepts filters, sorting options and pagination.
 * @bodyContent {MemberQuery} application/json
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.memberRead)

  await req.temporal.workflow.start('exportMembersToCSV', {
    taskQueue: 'exports',
    workflowId: `${TemporalWorkflowId.MEMBERS_CSV_EXPORTS}/${
      req.currentTenant.id
    }/${generateUUIDv4()}`,
    retry: {
      maximumAttempts: 1,
    },
    args: [
      {
        tenantId: req.currentTenant.id,
        segmentIds: req.body.segments,
        criteria: req.body,
        sendTo: [req.currentUser.email],
      } as ITriggerCSVExport,
    ],
    searchAttributes: {
      TenantId: [req.currentTenant.id],
    },
  })

  identifyTenant(req)

  track('Member CSV Export', {}, { ...req.body }, req.currentUser.id)

  await req.responseHandler.success(req, res, {})
}
