"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.get(`/user`, (0, errorMiddleware_1.safeWrap)(require('./userList').default));
    app.get(`/user/autocomplete`, (0, errorMiddleware_1.safeWrap)(require('./userAutocomplete').default));
    app.get(`/user/:id`, (0, errorMiddleware_1.safeWrap)(require('./userFind').default));
};
//# sourceMappingURL=index.js.map