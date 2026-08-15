"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.languageMiddleware = languageMiddleware;
function languageMiddleware(req, res, next) {
    req.language = req.headers['accept-language'] || 'en';
    return next();
}
//# sourceMappingURL=languageMiddleware.js.map