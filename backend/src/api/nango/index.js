"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const nango_1 = require("@crowd/nango");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
exports.default = async (app) => {
    if ((0, nango_1.NANGO_CLOUD_CONFIG)()) {
        await (0, nango_1.initNangoCloudClient)();
        app.get('/nango/session', (0, errorMiddleware_1.safeWrap)(async (req, res) => {
            const data = await (0, nango_1.getNangoCloudSessionToken)();
            await req.responseHandler.success(req, res, data);
        }));
    }
};
//# sourceMappingURL=index.js.map