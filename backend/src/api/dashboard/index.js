"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.get(`/dashboard`, (0, errorMiddleware_1.safeWrap)(require('./dashboardGet').default));
    app.get(`/dashboard/metrics`, (0, errorMiddleware_1.safeWrap)(require('./dashboardMetricsGet').default));
};
//# sourceMappingURL=index.js.map