"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
exports.default = (app) => {
    // Member Attributes
    app.get(`/member/:memberId/attributes`, (0, errorMiddleware_1.safeWrap)(require('./memberAttributesList').default));
    // Member Attributes Update
    app.patch(`/member/:memberId/attributes`, (0, errorMiddleware_1.safeWrap)(require('./memberAttributesUpdate').default));
};
//# sourceMappingURL=index.js.map