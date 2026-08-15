"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createRateLimiter = createRateLimiter;
const express_rate_limit_1 = __importDefault(require("express-rate-limit"));
const common_1 = require("@crowd/common");
function createRateLimiter({ max, windowMs, keyGenerator, skip: additionalSkip, }) {
    return (0, express_rate_limit_1.default)({
        max,
        windowMs,
        standardHeaders: true,
        ...(keyGenerator ? { keyGenerator } : {}),
        handler: (_req, res) => {
            const err = new common_1.RateLimitError();
            res.status(err.status).json(err.toJSON());
        },
        skip: (req) => req.method === 'OPTIONS' ||
            req.originalUrl.endsWith('/import') ||
            (additionalSkip ? additionalSkip(req) : false),
    });
}
//# sourceMappingURL=apiRateLimiter.js.map