"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.stewardshipsRouter = stewardshipsRouter;
const express_1 = require("express");
const apiRateLimiter_1 = require("@/api/apiRateLimiter");
const requireScopes_1 = require("@/api/public/middlewares/requireScopes");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const assignSteward_1 = require("./assignSteward");
const escalate_1 = require("./escalate");
const openStewardship_1 = require("./openStewardship");
const updateStatus_1 = require("./updateStatus");
const rateLimiter = (0, apiRateLimiter_1.createRateLimiter)({ max: 60, windowMs: 60 * 1000 });
// TODO[deprecate]: superseded by /v1/akrites/stewardships — remove once consumers have migrated
function stewardshipsRouter() {
    const router = (0, express_1.Router)();
    router.use(rateLimiter);
    router.post('/', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(openStewardship_1.openStewardship));
    router.put('/:id/steward', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(assignSteward_1.assignStewardHandler));
    router.put('/:id/escalate', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(escalate_1.escalateHandler));
    router.put('/:id/status', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_STEWARDSHIPS]), (0, errorMiddleware_1.safeWrap)(updateStatus_1.updateStatusHandler));
    return router;
}
//# sourceMappingURL=index.js.map