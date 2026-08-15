"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/customview`, (0, errorMiddleware_1.safeWrap)(require('./customViewCreate').default));
    app.put(`/customview/:id`, (0, errorMiddleware_1.safeWrap)(require('./customViewUpdate').default));
    app.patch(`/customview`, (0, errorMiddleware_1.safeWrap)(require('./customViewUpdateBulk').default));
    app.delete(`/customview`, (0, errorMiddleware_1.safeWrap)(require('./customViewDestroy').default));
    app.get(`/customview`, (0, errorMiddleware_1.safeWrap)(require('./customViewQuery').default));
};
//# sourceMappingURL=index.js.map