"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.get(`/mergeActions`, (0, errorMiddleware_1.safeWrap)(require('./mergeActionQuery').default));
};
//# sourceMappingURL=index.js.map