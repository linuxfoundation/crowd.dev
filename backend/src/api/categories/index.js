"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
exports.default = (app) => {
    // Category groups routes
    app.post('/category-group', (0, errorMiddleware_1.safeWrap)(require('./categoryGroupCreate').default));
    app.get('/category-group', (0, errorMiddleware_1.safeWrap)(require('./categoryGroupList').default));
    app.patch('/category-group/:id', (0, errorMiddleware_1.safeWrap)(require('./categoryGroupUpdate').default));
    app.delete('/category-group/:id', (0, errorMiddleware_1.safeWrap)(require('./categoryGroupDelete').default));
    // Categories routes
    app.post('/category', (0, errorMiddleware_1.safeWrap)(require('./categoryCreate').default));
    app.get('/category', (0, errorMiddleware_1.safeWrap)(require('./categoryList').default));
    app.patch('/category/:id', (0, errorMiddleware_1.safeWrap)(require('./categoryUpdate').default));
    app.delete('/category/:id', (0, errorMiddleware_1.safeWrap)(require('./categoryDelete').default));
    app.delete('/category', (0, errorMiddleware_1.safeWrap)(require('./categoryBulkDelete').default));
};
//# sourceMappingURL=index.js.map