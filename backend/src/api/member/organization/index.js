"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
exports.default = (app) => {
    // Member Organiaztion List
    app.get(`/member/:memberId/organization`, (0, errorMiddleware_1.safeWrap)(require('./memberOrganizationList').default));
    // Member Organiaztion Create
    app.post(`/member/:memberId/organization`, (0, errorMiddleware_1.safeWrap)(require('./memberOrganizationCreate').default));
    // Member Organiaztion Update
    app.patch(`/member/:memberId/organization/:id`, (0, errorMiddleware_1.safeWrap)(require('./memberOrganizationUpdate').default));
    // Member Organiaztion Delete
    app.delete(`/member/:memberId/organization/:id`, (0, errorMiddleware_1.safeWrap)(require('./memberOrganizationDelete').default));
};
//# sourceMappingURL=index.js.map