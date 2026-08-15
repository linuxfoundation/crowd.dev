"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.osspreyRouter = osspreyRouter;
const express_1 = require("express");
const requireScopes_1 = require("@/api/public/middlewares/requireScopes");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const activityFeed_1 = require("./activityFeed");
const metrics_1 = require("./metrics");
const packageList_1 = require("./packageList");
const packageScatter_1 = require("./packageScatter");
// TODO[deprecate]: superseded by /v1/akrites — ossprey endpoints are now at /v1/akrites/metrics,
// /v1/akrites/packages, /v1/akrites/packages/scatter, /v1/akrites/activity — remove once consumers have migrated
function osspreyRouter() {
    const router = (0, express_1.Router)();
    router.get('/metrics', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(metrics_1.metricsHandler));
    // /packages/scatter must be registered before /packages to avoid Express treating 'scatter' as a path param
    router.get('/packages/scatter', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(packageScatter_1.packageScatterHandler));
    router.get('/packages', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(packageList_1.packageListHandler));
    router.get('/activity', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(activityFeed_1.activityFeedHandler));
    return router;
}
//# sourceMappingURL=index.js.map