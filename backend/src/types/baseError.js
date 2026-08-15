"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BaseError = void 0;
class BaseError extends Error {
    constructor(message, originalError) {
        super(message);
        this.name = this.constructor.name;
        this.message = message;
        this.originalError = originalError;
        Error.captureStackTrace(this, this.constructor);
    }
}
exports.BaseError = BaseError;
//# sourceMappingURL=baseError.js.map