"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/product/event`, (0, errorMiddleware_1.safeWrap)(require('./productEventCreate').default));
    app.post(`/product/session`, (0, errorMiddleware_1.safeWrap)(require('./productSessionCreate').default));
    app.put(`/product/session/:id`, (0, errorMiddleware_1.safeWrap)(require('./productSessionUpdate').default));
};
//# sourceMappingURL=index.js.map