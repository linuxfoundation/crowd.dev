"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
function isInvalid(results, key) {
    if (results.value) {
        return !(key in results.value);
    }
    if (results.error) {
        throw results.error;
    }
    return true;
}
exports.default = isInvalid;
//# sourceMappingURL=isInvalid.js.map