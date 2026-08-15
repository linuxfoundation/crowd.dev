"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.opensearchMiddleware = opensearchMiddleware;
function opensearchMiddleware(cli) {
    return async (req, res, next) => {
        req.opensearch = cli;
        next();
    };
}
//# sourceMappingURL=opensearchMiddleware.js.map