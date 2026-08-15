"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/activity/query`, (0, errorMiddleware_1.safeWrap)(require('./activityQuery').default));
    app.get(`/activity/type`, (0, errorMiddleware_1.safeWrap)(require('./activityTypes').default));
    app.get(`/activity/channel`, (0, errorMiddleware_1.safeWrap)(require('./activityChannels').default));
    app.post('/activity/with-member', (0, errorMiddleware_1.safeWrap)(require('./activityAddWithMember').default));
};
//# sourceMappingURL=index.js.map