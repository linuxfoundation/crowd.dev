"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.get(`/data-quality/member`, (0, errorMiddleware_1.safeWrap)(require('./dataQualityMember').default));
    app.get(`/data-quality/organization`, (0, errorMiddleware_1.safeWrap)(require('./dataQualityOrganization').default));
};
//# sourceMappingURL=index.js.map