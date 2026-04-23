import { generateUUIDv4 } from '@crowd/common'
import { ITriggerCSVExport, TemporalWorkflowId } from '@crowd/types'

import Permissions from '../../security/permissions'
import identifyTenant from '../../segment/identifyTenant'
import track from '../../segment/track'
import PermissionChecker from '../../services/user/permissionChecker'

/**
 * POST /organization/export
 * @summary Export organizations as CSV
 * @tag Organizations
 * @security Bearer
 * @description Export organizations. It accepts filters, sorting options and pagination.
 * @bodyContent {OrganizationQuery} application/json
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.organizationRead)

  await req.temporal.workflow.start('exportOrganizationsToCSV', {
    taskQueue: 'exports',
    workflowId: `${TemporalWorkflowId.ORGANIZATIONS_CSV_EXPORTS}/${
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

  track('Organization CSV Export', {}, { ...req.body }, req.currentUser.id)

  await req.responseHandler.success(req, res, {})
}
