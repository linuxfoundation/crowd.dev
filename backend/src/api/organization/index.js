"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/organization`, (0, errorMiddleware_1.safeWrap)(require('./organizationCreate').default));
    app.post(`/organization/query`, (0, errorMiddleware_1.safeWrap)(require('./organizationQuery').default));
    app.put(`/organization/:id`, (0, errorMiddleware_1.safeWrap)(require('./organizationUpdate').default));
    app.delete(`/organization`, (0, errorMiddleware_1.safeWrap)(require('./organizationDestroy').default));
    app.post(`/organization/autocomplete`, (0, errorMiddleware_1.safeWrap)(require('./organizationAutocomplete').default));
    app.get(`/organization/:id`, (0, errorMiddleware_1.safeWrap)(require('./organizationFind').default));
    app.put(`/organization/:organizationId/merge`, (0, errorMiddleware_1.safeWrap)(require('./organizationMerge').default));
    app.put(`/organization/:organizationId/no-merge`, (0, errorMiddleware_1.safeWrap)(require('./organizationNotMerge').default));
    app.get(`/organization/:organizationId/can-revert-merge`, (0, errorMiddleware_1.safeWrap)(require('./organizationCanRevertMerge').default));
    app.post(`/organization/:organizationId/unmerge/preview`, (0, errorMiddleware_1.safeWrap)(require('./organizationUnmergePreview').default));
    app.post(`/organization/:organizationId/unmerge`, (0, errorMiddleware_1.safeWrap)(require('./organizationUnmerge').default));
    app.post(`/organization/export`, (0, errorMiddleware_1.safeWrap)(require('./organizationExport').default));
    app.post(`/organization/id`, (0, errorMiddleware_1.safeWrap)(require('./organizationByIds').default));
    // list organizations across all segments
    app.post(`/organization/list`, (0, errorMiddleware_1.safeWrap)(require('./organizationList').default));
    app.post(`/organization/:id/data-issue`, (0, errorMiddleware_1.safeWrap)(require('./organizationDataIssueCreate').default));
};
//# sourceMappingURL=index.js.map