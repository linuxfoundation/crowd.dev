"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Operator = void 0;
var Operator;
(function (Operator) {
    Operator["GREATER_THAN"] = "gt";
    Operator["GREATER_THAN_OR_EQUAL"] = "gte";
    Operator["LESS_THAN"] = "lt";
    Operator["LESS_THAN_OR_EQUAL"] = "lte";
    Operator["NOT_EQUAL"] = "ne";
    Operator["EQUAL"] = "eq";
    // case insensitive ilike
    Operator["LIKE"] = "like";
    Operator["NOT_LIKE"] = "notLike";
    Operator["TEXT_CONTAINS"] = "textContains";
    Operator["NOT_TEXT_CONTAINS"] = "notContains";
    Operator["REGEX"] = "regexp";
    Operator["NOT_REGEX"] = "notRegexp";
    Operator["AND"] = "and";
    Operator["OR"] = "or";
    Operator["IN"] = "in";
    Operator["NOT_IN"] = "notIn";
    Operator["BETWEEN"] = "between";
    Operator["NOT_BETWEEN"] = "notBetween";
    Operator["OVERLAP"] = "overlap";
    Operator["CONTAINS"] = "contains";
    Operator["NOT"] = "not";
})(Operator || (exports.Operator = Operator = {}));
//# sourceMappingURL=queryTypes.js.map