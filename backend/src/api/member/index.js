"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/member/query`, (0, errorMiddleware_1.safeWrap)(require('./memberQuery').default));
    app.post(`/member/export`, (0, errorMiddleware_1.safeWrap)(require('./memberExport').default));
    app.post(`/member`, (0, errorMiddleware_1.safeWrap)(require('./memberCreate').default));
    app.put(`/member/:id`, (0, errorMiddleware_1.safeWrap)(require('./memberUpdate').default));
    app.delete(`/member`, (0, errorMiddleware_1.safeWrap)(require('./memberDestroy').default));
    app.post(`/member/autocomplete`, (0, errorMiddleware_1.safeWrap)(require('./memberAutocomplete').default));
    app.get(`/member/bot-suggestions`, (0, errorMiddleware_1.safeWrap)(require('./memberBotSuggestionsList').default));
    app.get(`/member/:id`, (0, errorMiddleware_1.safeWrap)(require('./memberFind').default));
    app.get(`/member/github/:id`, (0, errorMiddleware_1.safeWrap)(require('./memberFindGithub').default));
    app.put(`/member/:memberId/merge`, (0, errorMiddleware_1.safeWrap)(require('./memberMerge').default));
    app.get(`/member/:memberId/can-revert-merge`, (0, errorMiddleware_1.safeWrap)(require('./memberCanRevertMerge').default));
    app.post(`/member/:memberId/unmerge/preview`, (0, errorMiddleware_1.safeWrap)(require('./memberUnmergePreview').default));
    app.post(`/member/:memberId/unmerge`, (0, errorMiddleware_1.safeWrap)(require('./memberUnmerge').default));
    app.put(`/member/:memberId/no-merge`, (0, errorMiddleware_1.safeWrap)(require('./memberNotMerge').default));
    app.patch(`/member`, (0, errorMiddleware_1.safeWrap)(require('./memberUpdateBulk').default));
    require('./identity').default(app);
    require('./organization').default(app);
    require('./attributes').default(app);
    require('./affiliation').default(app);
    app.post(`/member/:id/data-issue`, (0, errorMiddleware_1.safeWrap)(require('./memberDataIssueCreate').default));
};
//# sourceMappingURL=index.js.map