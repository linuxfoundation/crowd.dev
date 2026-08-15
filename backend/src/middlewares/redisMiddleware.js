"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.redisMiddleware = redisMiddleware;
function redisMiddleware(redis) {
    return async (req, res, next) => {
        req.redis = redis;
        next();
    };
}
//# sourceMappingURL=redisMiddleware.js.map