import {
  ActorType,
  AuditLogAction,
  AuditLogRequestOptions,
  addAuditAction,
} from '@crowd/data-access-layer'

import { IS_PROD_ENV, SERVICE, generateUUIDv1 } from '../../common/src'

import { BuildActionFn } from './baseActions'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function convertRepositoryOptions(options: any): AuditLogRequestOptions {
  const actor = options.actor ?? {
    id: options.currentUser?.id,
    type: ActorType.USER,
  }

  return {
    actorId: actor?.id,
    actorType: actor?.type,
    ipAddress: options.userData?.ip,
    userAgent: options.userData?.userAgent,
    requestId: options.requestId,
  }
}

async function captureChange(
  options: AuditLogRequestOptions,
  action: AuditLogAction,
  skipAuditLog: boolean,
  error?: Error,
): Promise<void> {
  if (skipAuditLog) {
    return
  }

  if (action.success && (!action.diff || JSON.stringify(action.diff) === '{}')) {
    // we ignore empty diffs but only if the action was successful
    // because when the action fails, the diff will be empty, but we need to log it
    return
  }

  if (error) {
    // log only error name and message
    action.error = {
      name: error.name,
      message: error.message,
    }
  }

  await addAuditAction(options, action)
}

export async function captureApiChange<T>(
  options: any, // eslint-disable-line @typescript-eslint/no-explicit-any
  buildActionFn: BuildActionFn<T>,
  skipAuditLog = false,
): Promise<T> {
  let skip = skipAuditLog

  let auditOptions: AuditLogRequestOptions
  if (options) {
    auditOptions = convertRepositoryOptions(options)
  } else if (process.env.CROWD_API_SERVICE_USER_ID) {
    auditOptions = {
      actorId: process.env.CROWD_API_SERVICE_USER_ID,
      actorType: ActorType.SERVICE,
      requestId: generateUUIDv1(),
      ipAddress: '127.0.0.1',
      userAgent: SERVICE,
    }
  } else if (!IS_PROD_ENV) {
    skip = true
  }

  const buildActionResult = await buildActionFn()
  try {
    await captureChange(auditOptions, buildActionResult.auditLog, skip, buildActionResult.error)
  } catch (error) {
    throw new Error(`Error capturing change: ${error.message}`)
  }

  if (buildActionResult.error) {
    throw buildActionResult.error
  }

  return buildActionResult.result
}
