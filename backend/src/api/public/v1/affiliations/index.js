"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.memberOrganizationAffiliationsRouter = memberOrganizationAffiliationsRouter;
const express_1 = require("express");
const apiRateLimiter_1 = require("@/api/apiRateLimiter");
const requireScopes_1 = require("@/api/public/middlewares/requireScopes");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const getAffiliationByHandle_1 = require("./getAffiliationByHandle");
const getAffiliations_1 = require("./getAffiliations");
const rateLimiter = (0, apiRateLimiter_1.createRateLimiter)({ max: 60, windowMs: 60 * 1000 });
function memberOrganizationAffiliationsRouter() {
    const router = (0, express_1.Router)();
    router.use(rateLimiter);
    router.post('/', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_AFFILIATIONS]), (0, errorMiddleware_1.safeWrap)(getAffiliations_1.getAffiliations));
    router.get('/:githubHandle', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_AFFILIATIONS]), (0, errorMiddleware_1.safeWrap)(getAffiliationByHandle_1.getAffiliationByHandle));
    return router;
}
//# sourceMappingURL=index.js.map