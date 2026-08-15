"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.akritesRouter = akritesRouter;
const express_1 = require("express");
const apiRateLimiter_1 = require("@/api/apiRateLimiter");
const requireScopes_1 = require("@/api/public/middlewares/requireScopes");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const activityFeed_1 = require("../ossprey/activityFeed");
const metrics_1 = require("../ossprey/metrics");
const packageList_1 = require("../ossprey/packageList");
const packageScatter_1 = require("../ossprey/packageScatter");
const batchGetStewardship_1 = require("../packages/batchGetStewardship");
const getPackage_1 = require("../packages/getPackage");
const getPackageAdvisories_1 = require("../packages/getPackageAdvisories");
const getPackageHistory_1 = require("../packages/getPackageHistory");
const getPackagesMetrics_1 = require("../packages/getPackagesMetrics");
const assignSteward_1 = require("../stewardships/assignSteward");
const escalate_1 = require("../stewardships/escalate");
const getMyActivity_1 = require("../stewardships/getMyActivity");
const getMyPackages_1 = require("../stewardships/getMyPackages");
const openStewardship_1 = require("../stewardships/openStewardship");
const updateStatus_1 = require("../stewardships/updateStatus");
const rateLimiter = (0, apiRateLimiter_1.createRateLimiter)({ max: 60, windowMs: 60 * 1000 });
function akritesRouter() {
    const router = (0, express_1.Router)();
    router.get('/metrics', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(metrics_1.metricsHandler));
    // /packages/scatter registered before router.use('/packages', ...) so Express evaluates this
    // explicit route first; without this ordering the sub-router would receive the request first
    // and call next() on no match, adding unnecessary overhead.
    router.get('/packages/scatter', rateLimiter, (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(packageScatter_1.packageScatterHandler));
    router.get('/packages', rateLimiter, (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(packageList_1.packageListHandler));
    router.get('/activity', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(activityFeed_1.activityFeedHandler));
    // --- packages ---
    router.post(/^\/packages:batch-stewardship\/?$/, rateLimiter, (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(batchGetStewardship_1.batchGetStewardship));
    const packagesSubRouter = (0, express_1.Router)();
    packagesSubRouter.use(rateLimiter);
    packagesSubRouter.get('/metrics', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(getPackagesMetrics_1.getPackagesMetrics));
    packagesSubRouter.get('/detail', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(getPackage_1.getPackage));
    packagesSubRouter.get('/advisories', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(getPackageAdvisories_1.getPackageAdvisories));
    packagesSubRouter.get('/history', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'), (0, errorMiddleware_1.safeWrap)(getPackageHistory_1.getPackageHistory));
    router.use('/packages', packagesSubRouter);
    // --- stewardships ---
    const stewardshipsSubRouter = (0, express_1.Router)();
    stewardshipsSubRouter.use(rateLimiter);
    stewardshipsSubRouter.get('/me/packages', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(getMyPackages_1.getMyPackagesHandler));
    stewardshipsSubRouter.get('/me/activity', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(getMyActivity_1.getMyActivityHandler));
    stewardshipsSubRouter.post('/open', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(openStewardship_1.openStewardship));
    stewardshipsSubRouter.post('/:id/assign', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(assignSteward_1.assignStewardHandler));
    stewardshipsSubRouter.post('/:id/escalate', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(escalate_1.escalateHandler));
    stewardshipsSubRouter.patch('/:id/status', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(updateStatus_1.updateStatusHandler));
    router.use('/stewardships', stewardshipsSubRouter);
    return router;
}
//# sourceMappingURL=index.js.map