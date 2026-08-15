"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
exports.default = (app) => {
    // Member Identity List
    app.get(`/member/:memberId/identity`, (0, errorMiddleware_1.safeWrap)(require('./memberIdentityList').default));
    // Member Identity Create
    app.post(`/member/:memberId/identity`, (0, errorMiddleware_1.safeWrap)(require('./memberIdentityCreate').default));
    // Member Identity Create Multiple
    app.put(`/member/:memberId/identity`, (0, errorMiddleware_1.safeWrap)(require('./memberIdentityCreateMultiple').default));
    // Member Identity Update
    app.patch(`/member/:memberId/identity/:id`, (0, errorMiddleware_1.safeWrap)(require('./memberIdentityUpdate').default));
    // Member Identity Delete
    app.delete(`/member/:memberId/identity/:id`, (0, errorMiddleware_1.safeWrap)(require('./memberIdentityDelete').default));
};
//# sourceMappingURL=index.js.map