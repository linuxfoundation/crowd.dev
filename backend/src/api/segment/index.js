"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    app.post(`/segment/projectGroup`, (0, errorMiddleware_1.safeWrap)(require('./segmentCreateProjectGroup').default));
    app.post(`/segment/project`, (0, errorMiddleware_1.safeWrap)(require('./segmentCreateProject').default));
    app.post(`/segment/subproject`, (0, errorMiddleware_1.safeWrap)(require('./segmentCreateSubproject').default));
    // query all project groups
    app.post(`/segment/projectGroup/query`, (0, errorMiddleware_1.safeWrap)(require('./segmentProjectGroupQuery').default));
    // query all projects
    app.post(`/segment/project/query`, (0, errorMiddleware_1.safeWrap)(require('./segmentProjectQuery').default));
    // query all subprojects
    app.post(`/segment/subproject/query`, (0, errorMiddleware_1.safeWrap)(require('./segmentSubprojectQuery').default));
    // query all subprojects lite
    app.post(`/segment/subproject/query-lite`, (0, errorMiddleware_1.safeWrap)(require('./segmentSubprojectQueryLite').default));
    // get segment by id
    app.get(`/segment/:segmentId`, (0, errorMiddleware_1.safeWrap)(require('./segmentFind').default));
    app.put(`/segment/:segmentId`, (0, errorMiddleware_1.safeWrap)(require('./segmentUpdate').default));
    // Multiple ids
    app.post(`/segment/id`, (0, errorMiddleware_1.safeWrap)(require('./segmentByIds').default));
};
//# sourceMappingURL=index.js.map