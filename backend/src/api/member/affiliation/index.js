"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
exports.default = (app) => {
    // Member Affiliation List
    app.get(`/member/:memberId/affiliation`, (0, errorMiddleware_1.safeWrap)(require('./memberAffiliationList').default));
    // Member Affiliation Create Multiple
    app.patch(`/member/:memberId/affiliation`, (0, errorMiddleware_1.safeWrap)(require('./memberAffiliationUpdateMultiple').default));
    app.post(`/member/:memberId/affiliation/override`, (0, errorMiddleware_1.safeWrap)(require('./memberAffiliationChangeOverride').default));
};
//# sourceMappingURL=index.js.map