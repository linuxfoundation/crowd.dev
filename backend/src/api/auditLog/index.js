"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/audit-logs/query`, (0, errorMiddleware_1.safeWrap)(require('./auditLogsQuery').default));
};
//# sourceMappingURL=index.js.map