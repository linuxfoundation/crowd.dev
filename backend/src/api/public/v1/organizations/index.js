"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.organizationsRouter = organizationsRouter;
const express_1 = require("express");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const requireScopes_1 = require("../../middlewares/requireScopes");
const createOrganization_1 = require("./createOrganization");
const getOrganization_1 = require("./getOrganization");
function organizationsRouter() {
    const router = (0, express_1.Router)();
    router.get('/', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_ORGANIZATIONS]), (0, errorMiddleware_1.safeWrap)(getOrganization_1.getOrganization));
    router.post('/', (0, requireScopes_1.requireScopes)([scopes_1.SCOPES.WRITE_ORGANIZATIONS]), (0, errorMiddleware_1.safeWrap)(createOrganization_1.createOrganization));
    return router;
}
//# sourceMappingURL=index.js.map