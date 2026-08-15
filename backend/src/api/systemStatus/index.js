"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.get(`/system-status`, (0, errorMiddleware_1.safeWrap)(require('./systemStatus').default));
};
//# sourceMappingURL=index.js.map