"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.publicRouter = publicRouter;
const express_1 = require("express");
const errorHandler_1 = require("./middlewares/errorHandler");
const v1_1 = require("./v1");
function publicRouter() {
    const router = (0, express_1.Router)();
    router.use('/v1', (0, v1_1.v1Router)());
    router.use(errorHandler_1.errorHandler);
    return router;
}
//# sourceMappingURL=index.js.map