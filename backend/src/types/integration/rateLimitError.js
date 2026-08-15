"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RateLimitError = void 0;
const baseError_1 = require("../baseError");
class RateLimitError extends baseError_1.BaseError {
    constructor(rateLimitResetSeconds, endpoint, origError) {
        super(`Endpoint: '${endpoint}' rate limit exceeded`, origError);
        this.rateLimitResetSeconds = rateLimitResetSeconds;
    }
}
exports.RateLimitError = RateLimitError;
//# sourceMappingURL=rateLimitError.js.map