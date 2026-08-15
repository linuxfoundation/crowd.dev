"use strict";
/* eslint-disable @typescript-eslint/no-unused-vars */
Object.defineProperty(exports, "__esModule", { value: true });
exports.mergeUniqueStringArrayItems = exports.keepPrimaryIfExists = exports.keepPrimary = void 0;
const keepPrimary = (primary, secondary) => primary;
exports.keepPrimary = keepPrimary;
const keepPrimaryIfExists = (primary, secondary) => {
    if (typeof primary === 'object') {
        if ((Array.isArray(primary) && primary.length > 0) ||
            (!Array.isArray(primary) && primary && Object.keys(primary).length > 0)) {
            return primary;
        }
    }
    else if (primary) {
        return primary;
    }
    return secondary;
};
exports.keepPrimaryIfExists = keepPrimaryIfExists;
const mergeUniqueStringArrayItems = (primary, secondary) => {
    if (!primary || primary.length === 0) {
        return secondary;
    }
    if (!secondary || secondary.length === 0) {
        return primary;
    }
    return [...new Set([...primary, ...secondary])];
};
exports.mergeUniqueStringArrayItems = mergeUniqueStringArrayItems;
//# sourceMappingURL=mergeFunctions.js.map