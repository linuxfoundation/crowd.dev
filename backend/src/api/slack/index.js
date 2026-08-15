"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("../../conf/index");
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    if (index_1.SLACK_CONFIG.appId && index_1.SLACK_CONFIG.appToken && index_1.SLACK_CONFIG.teamId) {
        app.post('/slack/commands', (0, errorMiddleware_1.safeWrap)(require('./command').default));
    }
};
//# sourceMappingURL=index.js.map