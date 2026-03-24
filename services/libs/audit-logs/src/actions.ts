import { diff } from 'deep-object-diff'

import { ActionType } from '@crowd/data-access-layer'

import { type BuildActionFn, type CaptureFn, CaptureOneFn, createCaptureFn } from './baseActions'

function modifyEntityAction<T>(
  actionType: ActionType,
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return async () => {
    const oldState = createCaptureFn()
    const newState = createCaptureFn()

    const createAuditLog = () => ({
      actionType,
      entityId,
      oldState: oldState.value,
      newState: newState.value,
      diff: diff(oldState.value, newState.value),
    })

    try {
      const result = await captureFn(oldState.fn, newState.fn)
      return {
        result,
        auditLog: {
          ...createAuditLog(),
          success: true,
        },
      }
    } catch (error) {
      return {
        result: null,
        error,
        auditLog: {
          ...createAuditLog(),
          success: false,
        },
      }
    }
  }
}

function createEntityAction<T>(
  actionType: ActionType,
  entityId: string,
  captureFn: CaptureOneFn<T>,
): BuildActionFn<T> {
  return async () => {
    const newState = createCaptureFn()

    const createAuditLog = () => ({
      actionType,
      entityId,
      oldState: {},
      newState: newState.value,
      diff: newState.value,
    })

    try {
      const result = await captureFn(newState.fn)
      return {
        result,
        auditLog: {
          ...createAuditLog(),
          success: true,
        },
      }
    } catch (error) {
      return {
        result: null,
        error,
        auditLog: {
          ...createAuditLog(),
          success: false,
        },
      }
    }
  }
}

export function memberEditProfileAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.MEMBERS_EDIT_PROFILE, entityId, captureFn)
}

export function memberVerifyIdentityAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.MEMBERS_VERIFY_IDENTITY, entityId, captureFn)
}

export function memberVerifyWorkExperienceAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.MEMBERS_VERIFY_WORK_EXPERIENCE, entityId, captureFn)
}

export function memberMergeAction<T>(entityId: string, captureFn: CaptureFn<T>): BuildActionFn<T> {
  return modifyEntityAction(ActionType.MEMBERS_MERGE, entityId, captureFn)
}

export function memberUnmergeAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.MEMBERS_UNMERGE, entityId, captureFn)
}

export function memberCreateAction<T>(
  entityId: string,
  captureFn: CaptureOneFn<T>,
): BuildActionFn<T> {
  return createEntityAction(ActionType.MEMBERS_CREATE, entityId, captureFn)
}

export function organizationCreateAction<T>(
  entityId: string,
  captureFn: CaptureOneFn<T>,
): BuildActionFn<T> {
  return createEntityAction(ActionType.ORGANIZATIONS_CREATE, entityId, captureFn)
}

export function organizationUpdateAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.ORGANIZATIONS_EDIT_PROFILE, entityId, captureFn)
}

export function integrationConnectAction<T>(
  entityId: string,
  captureFn: CaptureOneFn<T>,
): BuildActionFn<T> {
  return createEntityAction(ActionType.INTEGRATIONS_CONNECT, entityId, captureFn)
}

export function memberEditIdentitiesAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.MEMBERS_EDIT_IDENTITIES, entityId, captureFn)
}

export function memberEditOrganizationsAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.MEMBERS_EDIT_ORGANIZATIONS, entityId, captureFn)
}

export function organizationEditIdentitiesAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.ORGANIZATIONS_EDIT_IDENTITIES, entityId, captureFn)
}

export function organizationMergeAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.ORGANIZATIONS_MERGE, entityId, captureFn)
}

export function organizationUnmergeAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.ORGANIZATIONS_UNMERGE, entityId, captureFn)
}

export function memberEditAffiliationsAction<T>(
  entityId: string,
  captureFn: CaptureFn<T>,
): BuildActionFn<T> {
  return modifyEntityAction(ActionType.MEMBERS_EDIT_MANUAL_AFFILIATION, entityId, captureFn)
}
