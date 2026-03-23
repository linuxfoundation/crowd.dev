import { Router } from 'express'

import { requireScopes } from '@/api/public/middlewares/requireScopes'
import { safeWrap } from '@/middlewares/errorMiddleware'
import { SCOPES } from '@/security/scopes'

import { getMemberIdentities } from './identities/getMemberIdentities'
import { verifyMemberIdentity } from './identities/verifyMemberIdentity'
import { getMemberMaintainerRoles } from './maintainer-roles/getMemberMaintainerRoles'
import { getProjectAffiliations } from './project-affiliations/getProjectAffiliations'
import { patchProjectAffiliation } from './project-affiliations/patchProjectAffiliation'
import { resolveMemberByIdentities } from './resolveMember'
import { createMemberWorkExperience } from './work-experiences/createMemberWorkExperience'
import { deleteMemberWorkExperience } from './work-experiences/deleteMemberWorkExperience'
import { getMemberWorkExperiences } from './work-experiences/getMemberWorkExperiences'
import { updateMemberWorkExperience } from './work-experiences/updateMemberWorkExperience'
import { verifyMemberWorkExperience } from './work-experiences/verifyMemberWorkExperience'

export function membersRouter(): Router {
  const router = Router()

  router.post('/resolve', requireScopes([SCOPES.READ_MEMBERS]), safeWrap(resolveMemberByIdentities))

  router.get(
    '/:memberId/identities',
    requireScopes([SCOPES.READ_MEMBER_IDENTITIES]),
    safeWrap(getMemberIdentities),
  )

  router.patch(
    '/:memberId/identities/:identityId',
    requireScopes([SCOPES.WRITE_MEMBER_IDENTITIES]),
    safeWrap(verifyMemberIdentity),
  )

  router.get(
    '/:memberId/maintainer-roles',
    requireScopes([SCOPES.READ_MAINTAINER_ROLES]),
    safeWrap(getMemberMaintainerRoles),
  )

  router.get(
    '/:memberId/project-affiliations',
    requireScopes([SCOPES.READ_PROJECT_AFFILIATIONS]),
    safeWrap(getProjectAffiliations),
  )

  router.patch(
    '/:memberId/project-affiliations/:projectId',
    requireScopes([SCOPES.WRITE_PROJECT_AFFILIATIONS]),
    safeWrap(patchProjectAffiliation),
  )

  router.post(
    '/:memberId/work-experiences',
    requireScopes([SCOPES.WRITE_WORK_EXPERIENCES]),
    safeWrap(createMemberWorkExperience),
  )

  router.get(
    '/:memberId/work-experiences',
    requireScopes([SCOPES.READ_WORK_EXPERIENCES]),
    safeWrap(getMemberWorkExperiences),
  )

  router.put(
    '/:memberId/work-experiences/:workExperienceId',
    requireScopes([SCOPES.WRITE_WORK_EXPERIENCES]),
    safeWrap(updateMemberWorkExperience),
  )

  router.patch(
    '/:memberId/work-experiences/:workExperienceId',
    requireScopes([SCOPES.WRITE_WORK_EXPERIENCES]),
    safeWrap(verifyMemberWorkExperience),
  )

  router.delete(
    '/:memberId/work-experiences/:workExperienceId',
    requireScopes([SCOPES.WRITE_WORK_EXPERIENCES]),
    safeWrap(deleteMemberWorkExperience),
  )

  return router
}
