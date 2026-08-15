"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.errorHandler = void 0;
const express_oauth2_jwt_bearer_1 = require("express-oauth2-jwt-bearer");
const common_1 = require("@crowd/common");
const alertOnce_1 = require("@/api/public/alerts/alertOnce");
/**
 * Converts errors to structured JSON: `{ error: { code, message } }`.
 * Defaults to 500 Internal Error for unhandled errors.
 */
const errorHandler = (error, req, res, _next) => {
    if (error instanceof common_1.HttpError) {
        void (0, alertOnce_1.alertOnce)(req, {
            status: error.status,
            code: error.code,
            message: error.message,
            name: error.name,
            context: error.context,
            stack: error.status >= 500 ? error.stack : undefined,
        });
        res.status(error.status).json(error.toJSON());
        return;
    }
    if (error instanceof express_oauth2_jwt_bearer_1.InsufficientScopeError) {
        const httpErr = new common_1.InsufficientScopeError(error.message || undefined);
        res.status(httpErr.status).json(httpErr.toJSON());
        return;
    }
    if (error instanceof express_oauth2_jwt_bearer_1.UnauthorizedError) {
        const httpErr = new common_1.UnauthorizedError(error.message || undefined);
        res.status(httpErr.status).json(httpErr.toJSON());
        return;
    }
    req.log.error({
        error: { name: error === null || error === void 0 ? void 0 : error.name, message: error === null || error === void 0 ? void 0 : error.message, stack: error === null || error === void 0 ? void 0 : error.stack },
        url: req.url,
        method: req.method,
        query: req.query,
        body: req.body,
    }, 'Unhandled error in public API');
    void (0, alertOnce_1.alertOnce)(req, {
        status: 500,
        code: 'INTERNAL_ERROR',
        message: (error === null || error === void 0 ? void 0 : error.message) || 'No message',
        name: (error === null || error === void 0 ? void 0 : error.name) || 'Unknown',
        stack: error === null || error === void 0 ? void 0 : error.stack,
    });
    const unknownError = new common_1.InternalError();
    res.status(unknownError.status).json(unknownError.toJSON());
};
exports.errorHandler = errorHandler;
//# sourceMappingURL=errorHandler.js.map