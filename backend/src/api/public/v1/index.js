"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.v1Router = v1Router;
const express_1 = require("express");
const common_1 = require("@crowd/common");
const apiRateLimiter_1 = require("@/api/apiRateLimiter");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const conf_1 = require("../../../conf");
const oauth2Middleware_1 = require("../middlewares/oauth2Middleware");
const requireScopes_1 = require("../middlewares/requireScopes");
const staticApiKeyMiddleware_1 = require("../middlewares/staticApiKeyMiddleware");
const affiliations_1 = require("./affiliations");
const akrites_1 = require("./akrites");
const akrites_external_1 = require("./akrites-external");
const members_1 = require("./members");
const organizations_1 = require("./organizations");
const ossprey_1 = require("./ossprey");
const packages_1 = require("./packages");
const batchGetStewardship_1 = require("./packages/batchGetStewardship");
const stewardships_1 = require("./stewardships");
const packagesRateLimiter = (0, apiRateLimiter_1.createRateLimiter)({ max: 60, windowMs: 60 * 1000 });
function v1Router() {
    const router = (0, express_1.Router)();
    router.use('/members', (0, oauth2Middleware_1.oauth2Middleware)(conf_1.AUTH0_CONFIG), (0, members_1.membersRouter)());
    router.use('/organizations', (0, oauth2Middleware_1.oauth2Middleware)(conf_1.AUTH0_CONFIG), (0, organizations_1.organizationsRouter)());
    router.use('/affiliations', (0, staticApiKeyMiddleware_1.staticApiKeyMiddleware)(), (0, affiliations_1.memberOrganizationAffiliationsRouter)());
    // TODO[deprecate]: /packages, /stewardships, /ossprey are superseded by /akrites — remove once consumers have migrated
    router.post(/^\/packages:batch-stewardship\/?$/, (0, oauth2Middleware_1.oauth2Middleware)(conf_1.AUTH0_CONFIG), packagesRateLimiter, (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(batchGetStewardship_1.batchGetStewardship));
    router.use('/packages', (0, oauth2Middleware_1.oauth2Middleware)(conf_1.AUTH0_CONFIG), (0, packages_1.packagesRouter)());
    router.use('/stewardships', (0, oauth2Middleware_1.oauth2Middleware)(conf_1.AUTH0_CONFIG), (0, stewardships_1.stewardshipsRouter)());
    router.use('/ossprey', (0, oauth2Middleware_1.oauth2Middleware)(conf_1.AUTH0_CONFIG), (0, ossprey_1.osspreyRouter)());
    router.use('/akrites', (0, oauth2Middleware_1.oauth2Middleware)(conf_1.AUTH0_CONFIG), (0, akrites_1.akritesRouter)());
    router.use('/akrites-external', (0, oauth2Middleware_1.oauth2Middleware)(conf_1.AUTH0_CONFIG), (0, akrites_external_1.akritesExternalRouter)());
    router.use(() => {
        throw new common_1.NotFoundError();
    });
    return router;
}
//# sourceMappingURL=index.js.map