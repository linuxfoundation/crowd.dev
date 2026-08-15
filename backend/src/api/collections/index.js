"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
exports.default = (app) => {
    // Insights projects routes
    app.post('/collections/insights-projects/query', (0, errorMiddleware_1.safeWrap)(require('./insightsProjects/insightsProjectsQuery').default));
    app.post('/collections/insights-projects', (0, errorMiddleware_1.safeWrap)(require('./insightsProjects/insightsProjectsCreate').default));
    app.delete('/collections/insights-projects/:id', (0, errorMiddleware_1.safeWrap)(require('./insightsProjects/insightsProjectsDestroy').default));
    app.post('/collections/insights-projects/:id', (0, errorMiddleware_1.safeWrap)(require('./insightsProjects/insightsProjectsUpdate').default));
    app.get('/collections/insights-projects/:id', (0, errorMiddleware_1.safeWrap)(require('./insightsProjects/insightsProjectsGet').default));
    // Collections routes
    app.post('/collections/query', (0, errorMiddleware_1.safeWrap)(require('./collectionsQuery').default));
    app.post('/collections', (0, errorMiddleware_1.safeWrap)(require('./collectionsCreate').default));
    app.get('/collections/:id', (0, errorMiddleware_1.safeWrap)(require('./collectionsGet').default));
    app.post('/collections/:id', (0, errorMiddleware_1.safeWrap)(require('./collectionsUpdate').default));
    app.delete('/collections/:id', (0, errorMiddleware_1.safeWrap)(require('./collectionsDestroy').default));
    app.get('/segments/:id/repositories', (0, errorMiddleware_1.safeWrap)(require('./segmentsRepositoriesGet').default));
    app.get('/segments/:id/github-insights', (0, errorMiddleware_1.safeWrap)(require('./segmentsGithubInsightsGet').default));
    app.get('/segments/:id/widgets', (0, errorMiddleware_1.safeWrap)(require('./segmentsWidgetsGet').default));
};
//# sourceMappingURL=index.js.map