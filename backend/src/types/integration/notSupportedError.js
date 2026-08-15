"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.NotSupportedError = void 0;
const baseError_1 = require("../baseError");
class NotSupportedError extends baseError_1.BaseError {
    constructor(action, originalError) {
        super(`Action '${action}' is not supported!`, originalError);
        this.action = action;
    }
}
exports.NotSupportedError = NotSupportedError;
//# sourceMappingURL=notSupportedError.js.map