import {
  ActorType,
  AuditLogAction,
  AuditLogRequestOptions,
  addAuditAction,
} from '@crowd/data-access-layer'

import { IS_PROD_ENV, SERVICE, generateUUIDv1 } from '../../common/src'

import { BuildActionFn } from './baseActions'

export function buildAuditLogOptions(
  options?: {
    actor?: { id?: string | null; type?: ActorType }
    currentUser?: { id?: string | null }
    requestId?: string
    userData?: { ip?: string; userAgent?: string }
  } | null,
): AuditLogRequestOptions | null {
  const actorId = options?.actor?.id || options?.currentUser?.id

  // If an actor ID is provided, use it.
  if (actorId) {
    return {
      actorId,
      actorType: options?.actor?.type ?? ActorType.USER,
      requestId: options?.requestId ?? generateUUIDv1(),
      ipAddress: options?.userData?.ip,
      userAgent: options?.userData?.userAgent,
    }
  }

  // This is the fallback for workers that don't have an actor ID.
  if (process.env.CROWD_LF_AGENT_USER_ID) {
    return {
      actorId: process.env.CROWD_LF_AGENT_USER_ID,
      actorType: ActorType.SERVICE,
      requestId: options?.requestId ?? generateUUIDv1(),
      ipAddress: options?.userData?.ip ?? '127.0.0.1',
      userAgent: options?.userData?.userAgent ?? SERVICE,
    }
  }

  return null
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

  const auditOptions = buildAuditLogOptions(options)

  if (!auditOptions && !IS_PROD_ENV) {
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
