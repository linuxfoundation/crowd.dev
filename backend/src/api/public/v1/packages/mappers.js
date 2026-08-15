"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.snakeToCamelKeys = snakeToCamelKeys;
exports.toNullableNumber = toNullableNumber;
exports.repoMappingLabel = repoMappingLabel;
function snakeToCamelKeys(obj) {
    if (obj === null)
        return null;
    return Object.fromEntries(Object.entries(obj).map(([k, v]) => [k.replace(/_([a-z])/g, (_, c) => c.toUpperCase()), v]));
}
// pg-promise returns numeric columns as strings; this coerces without turning null into 0.
function toNullableNumber(value) {
    return value != null ? Number(value) : null;
}
function repoMappingLabel(confidence) {
    if (confidence === null)
        return null;
    if (confidence >= 0.8)
        return 'High';
    if (confidence >= 0.5)
        return 'Medium';
    return 'Low';
}
//# sourceMappingURL=mappers.js.map