"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.membersRouter = membersRouter;
const express_1 = require("express");
const apiRateLimiter_1 = require("@/api/apiRateLimiter");
const requireScopes_1 = require("@/api/public/middlewares/requireScopes");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const createMember_1 = require("./createMember");
const createMemberIdentity_1 = require("./identities/createMemberIdentity");
const getMemberIdentities_1 = require("./identities/getMemberIdentities");
const verifyMemberIdentity_1 = require("./identities/verifyMemberIdentity");
const getMemberMaintainerRoles_1 = require("./maintainer-roles/getMemberMaintainerRoles");
const getProjectAffiliations_1 = require("./project-affiliations/getProjectAffiliations");
const patchProjectAffiliation_1 = require("./project-affiliations/patchProjectAffiliation");
const resolveMember_1 = require("./resolveMember");
const createMemberWorkExperience_1 = require("./work-experiences/createMemberWorkExperience");
const deleteMemberWorkExperience_1 = require("./work-experiences/deleteMemberWorkExperience");
const getMemberWorkExperiences_1 = require("./work-experiences/getMemberWorkExperiences");
const updateMemberWorkExperience_1 = require("./work-experiences/updateMemberWorkExperience");
const verifyMemberWorkExperience_1 = require("./work-experiences/verifyMemberWorkExperience");
const resolveMemberRateLimiter = (0, apiRateLimiter_1.createRateLimiter)({
    max: 200,
    windowMs: 60 * 1000,
    keyGenerator: (req) => req.actor.id,
});
function membersRouter() {
    const router = (0, express_1.Router)();
    router.post('/', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_MEMBERS]), (0, errorMiddleware_1.safeWrap)(createMember_1.createMember));
    router.post('/resolve', resolveMemberRateLimiter, (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_MEMBERS]), (0, errorMiddleware_1.safeWrap)(resolveMember_1.resolveMemberByIdentities));
    router.get('/:memberId/identities', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_MEMBER_IDENTITIES]), (0, errorMiddleware_1.safeWrap)(getMemberIdentities_1.getMemberIdentities));
    router.post('/:memberId/identities', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_MEMBER_IDENTITIES]), (0, errorMiddleware_1.safeWrap)(createMemberIdentity_1.createMemberIdentity));
    router.patch('/:memberId/identities/:identityId', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_MEMBER_IDENTITIES]), (0, errorMiddleware_1.safeWrap)(verifyMemberIdentity_1.verifyMemberIdentity));
    router.get('/:memberId/maintainer-roles', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_MAINTAINER_ROLES]), (0, errorMiddleware_1.safeWrap)(getMemberMaintainerRoles_1.getMemberMaintainerRoles));
    router.get('/:memberId/project-affiliations', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PROJECT_AFFILIATIONS]), (0, errorMiddleware_1.safeWrap)(getProjectAffiliations_1.getProjectAffiliations));
    router.patch('/:memberId/project-affiliations/:projectId', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_PROJECT_AFFILIATIONS]), (0, errorMiddleware_1.safeWrap)(patchProjectAffiliation_1.patchProjectAffiliation));
    router.post('/:memberId/work-experiences', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_WORK_EXPERIENCES]), (0, errorMiddleware_1.safeWrap)(createMemberWorkExperience_1.createMemberWorkExperience));
    router.get('/:memberId/work-experiences', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_WORK_EXPERIENCES]), (0, errorMiddleware_1.safeWrap)(getMemberWorkExperiences_1.getMemberWorkExperiences));
    router.put('/:memberId/work-experiences/:workExperienceId', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_WORK_EXPERIENCES]), (0, errorMiddleware_1.safeWrap)(updateMemberWorkExperience_1.updateMemberWorkExperience));
    router.patch('/:memberId/work-experiences/:workExperienceId', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_WORK_EXPERIENCES]), (0, errorMiddleware_1.safeWrap)(verifyMemberWorkExperience_1.verifyMemberWorkExperience));
    router.delete('/:memberId/work-experiences/:workExperienceId', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_WORK_EXPERIENCES]), (0, errorMiddleware_1.safeWrap)(deleteMemberWorkExperience_1.deleteMemberWorkExperience));
    return router;
}
//# sourceMappingURL=index.js.map