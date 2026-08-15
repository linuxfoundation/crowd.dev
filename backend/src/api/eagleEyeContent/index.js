"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/eagleEyeContent/query`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeContentQuery').default));
    app.post(`/eagleEyeContent`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeContentUpsert').default));
    app.post(`/eagleEyeContent/track`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeContentTrack').default));
    app.get(`/eagleEyeContent/reply`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeContentReply').default));
    app.get(`/eagleEyeContent/search`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeContentSearch').default));
    app.get(`/eagleEyeContent/:id`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeContentFind').default));
    app.post(`/eagleEyeContent/:contentId/action`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeActionCreate').default));
    app.put(`/eagleEyeContent/settings`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeSettingsUpdate').default));
    app.delete(`/eagleEyeContent/:contentId/action/:actionId`, (0, errorMiddleware_1.safeWrap)(require('./eagleEyeActionDestroy').default));
};
//# sourceMappingURL=index.js.map