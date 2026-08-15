"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.packagesRouter = packagesRouter;
const express_1 = require("express");
const apiRateLimiter_1 = require("@/api/apiRateLimiter");
const requireScopes_1 = require("@/api/public/middlewares/requireScopes");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const getPackage_1 = require("./getPackage");
const getPackagesMetrics_1 = require("./getPackagesMetrics");
const listPackages_1 = require("./listPackages");
const rateLimiter = (0, apiRateLimiter_1.createRateLimiter)({ max: 60, windowMs: 60 * 1000 });
// TODO[deprecate]: /packages/metrics and /packages/detail are superseded by /v1/akrites/packages/metrics
// and /v1/akrites/packages/detail — remove once consumers have migrated.
// NOTE: GET /packages (listPackages) is intentionally NOT replicated in /v1/akrites because it has a
// different response shape from GET /v1/akrites/packages (ossprey packageListHandler). Before removing,
// verify no consumer calls GET /v1/packages — if unused, delete listPackages and this route entirely.
function packagesRouter() {
    const router = (0, express_1.Router)();
    router.use(rateLimiter);
    router.use((0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'all'));
    router.get('/', (0, errorMiddleware_1.safeWrap)(listPackages_1.listPackages));
    router.get('/metrics', (0, errorMiddleware_1.safeWrap)(getPackagesMetrics_1.getPackagesMetrics));
    router.get('/detail', (0, errorMiddleware_1.safeWrap)(getPackage_1.getPackage));
    return router;
}
//# sourceMappingURL=index.js.map