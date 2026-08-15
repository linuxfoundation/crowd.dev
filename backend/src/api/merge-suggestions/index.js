"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/membersToMerge`, (0, errorMiddleware_1.safeWrap)(require('./membersToMergeList').default));
    app.post(`/organizationsToMerge`, (0, errorMiddleware_1.safeWrap)(require('./organizationsToMergeList').default));
};
//# sourceMappingURL=index.js.map