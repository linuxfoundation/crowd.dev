"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const types_1 = require("@crowd/types");
const queryTypes_1 = require("./queryTypes");
class RawQueryParser {
    static parseFilters(filters, columnMap, jsonColumnInfos, params) {
        const keys = Object.keys(filters);
        if (keys.length === 0) {
            return '(1=1)';
        }
        const results = [];
        for (const key of keys) {
            if (this.isOperator(key)) {
                const operands = [];
                for (const operand of filters[key]) {
                    operands.push(this.parseFilters(operand, columnMap, jsonColumnInfos, params));
                }
                const condition = operands.join(` ${key} `);
                results.push(`(${condition})`);
            }
            else if (key === queryTypes_1.Operator.NOT) {
                const condition = this.parseFilters(filters[key], columnMap, jsonColumnInfos, params);
                results.push(`(not ${condition})`);
            }
            else {
                const jsonColumnInfo = this.getJsonColumnInfo(key, jsonColumnInfos);
                if (jsonColumnInfo === undefined && !columnMap.has(key)) {
                    throw new Error(`Unknown filter key: ${key}!`);
                }
                if (jsonColumnInfo) {
                    results.push(this.parseJsonColumnCondition(jsonColumnInfo, filters[key], params));
                }
                else {
                    // handle column maps without quotes/alias to handle postgres camelCase columns
                    const column = columnMap.get(key).indexOf('"') === -1 ? `"${columnMap.get(key)}"` : columnMap.get(key);
                    results.push(this.parseColumnCondition(key, column, filters[key], params));
                }
            }
        }
        return results.join(' and ');
    }
    static parseJsonColumnCondition(property, filters, params) {
        const parts = property.parts.slice(1);
        let jsonColumn;
        if (parts.length > 0) {
            const attribute = parts[0];
            const attributeInfo = (0, common_1.singleOrDefault)(property.info.attributeInfos, (a) => a.name === attribute);
            if (attributeInfo === undefined) {
                throw new Error(`Unknown ${property.info.property} attribute: ${attribute}!`);
            }
            const isText = this.isJsonPropertyText(attributeInfo.type);
            let nestedProperty = '';
            for (let i = 0; i < parts.length; i++) {
                const part = parts[i];
                if (isText && i === parts.length - 1) {
                    nestedProperty += ` ->> '${part}'`;
                }
                else {
                    nestedProperty += ` -> '${part}'`;
                }
            }
            jsonColumn = `(${property.info.column}${nestedProperty})`;
            if (attributeInfo.type === types_1.MemberAttributeType.BOOLEAN) {
                jsonColumn = `${jsonColumn}::boolean`;
            }
            else if (attributeInfo.type === types_1.MemberAttributeType.NUMBER) {
                jsonColumn = `${jsonColumn}::integer`;
            }
            else if (attributeInfo.type === types_1.MemberAttributeType.DATE) {
                jsonColumn = `${jsonColumn}::timestamptz`;
            }
        }
        else {
            jsonColumn = `(${property.info.column})`;
        }
        let value;
        let operator;
        if (Array.isArray(filters)) {
            operator = queryTypes_1.Operator.CONTAINS;
            value = filters;
        }
        else {
            const conditionKeys = Object.keys(filters);
            if (conditionKeys.length !== 1) {
                throw new Error(`Invalid condition! ${JSON.stringify(filters, undefined, 2)}`);
            }
            operator = conditionKeys[0];
            value = filters[operator];
        }
        const actualOperator = this.getOperator(operator, true);
        if (operator === queryTypes_1.Operator.BETWEEN || operator === queryTypes_1.Operator.NOT_BETWEEN) {
            // we need two values
            const firstParamName = this.getJsonParamName(property.info.property, parts, params);
            params[firstParamName] = value[0];
            const secondParamName = this.getJsonParamName(property.info.property, parts, params);
            params[secondParamName] = value[1];
            return `(${jsonColumn} ${actualOperator} :${firstParamName} and :${secondParamName})`;
        }
        if (operator === queryTypes_1.Operator.CONTAINS || operator === queryTypes_1.Operator.OVERLAP) {
            // we need an array of values
            const paramNames = [];
            for (const val of value) {
                const paramName = this.getJsonParamName(property.info.property, parts, params);
                params[paramName] = val;
                paramNames.push(`:${paramName}`);
            }
            const paramNamesString = paramNames.join(', ');
            return `(${jsonColumn} ${actualOperator} array[${paramNamesString}])`;
        }
        if (operator === queryTypes_1.Operator.IN || operator === queryTypes_1.Operator.NOT_IN) {
            // we need a list of values
            const paramNames = [];
            for (const val of value) {
                const paramName = this.getJsonParamName(property.info.property, parts, params);
                params[paramName] = val;
                paramNames.push(`:${paramName}`);
            }
            const paramNamesString = paramNames.join(', ');
            return `(${jsonColumn} ${actualOperator} (${paramNamesString}))`;
        }
        const paramName = this.getJsonParamName(property.info.property, parts, params);
        if (operator === queryTypes_1.Operator.LIKE || operator === queryTypes_1.Operator.NOT_LIKE) {
            params[paramName] = `%${value}%`;
        }
        else {
            params[paramName] = value;
        }
        return `(${jsonColumn} ${actualOperator} :${paramName})`;
    }
    static parseColumnCondition(key, column, filters, params) {
        const conditionKeys = Object.keys(filters);
        if (conditionKeys.length !== 1) {
            throw new Error(`Invalid condition! ${JSON.stringify(filters, undefined, 2)}`);
        }
        const operator = conditionKeys[0];
        const actualOperator = this.getOperator(operator);
        const value = filters[operator];
        if (operator === queryTypes_1.Operator.BETWEEN || operator === queryTypes_1.Operator.NOT_BETWEEN) {
            // we need two values
            const firstParamName = this.getParamName(key, params);
            params[firstParamName] = value[0];
            const secondParamName = this.getParamName(key, params);
            params[secondParamName] = value[1];
            return `(${column} ${actualOperator} :${firstParamName} and :${secondParamName})`;
        }
        if (operator === queryTypes_1.Operator.CONTAINS || operator === queryTypes_1.Operator.OVERLAP) {
            // we need an array of values
            const paramNames = [];
            for (const val of value) {
                const paramName = this.getParamName(key, params);
                params[paramName] = val;
                paramNames.push(`:${paramName}`);
            }
            const paramNamesString = paramNames.join(', ');
            return `(${column} ${actualOperator} array[${paramNamesString}])`;
        }
        if (operator === queryTypes_1.Operator.IN || operator === queryTypes_1.Operator.NOT_IN) {
            // we need a list of values
            const paramNames = [];
            for (const val of value) {
                const paramName = this.getParamName(key, params);
                params[paramName] = val;
                paramNames.push(`:${paramName}`);
            }
            const paramNamesString = paramNames.join(', ');
            return `(${column} ${actualOperator} (${paramNamesString}))`;
        }
        const paramName = this.getParamName(key, params);
        if (operator === queryTypes_1.Operator.EQUAL &&
            (value === null || (typeof value === 'string' && value.toLowerCase() === 'null'))) {
            params[paramName] = null;
            return `(${column} is :${paramName})`;
        }
        if (operator === queryTypes_1.Operator.NOT_EQUAL &&
            (value === null || (typeof value === 'string' && value.toLowerCase() === 'null'))) {
            params[paramName] = null;
            return `(${column} is not :${paramName})`;
        }
        if (operator === queryTypes_1.Operator.LIKE ||
            operator === queryTypes_1.Operator.NOT_LIKE ||
            operator === queryTypes_1.Operator.TEXT_CONTAINS ||
            operator === queryTypes_1.Operator.NOT_TEXT_CONTAINS) {
            params[paramName] = `%${value}%`;
        }
        else {
            params[paramName] = value;
        }
        return `(${column} ${actualOperator} :${paramName})`;
    }
    static getJsonColumnInfo(column, jsonColumnInfos) {
        const parts = column.split('.');
        const actualProperty = parts[0];
        const info = (0, common_1.singleOrDefault)(jsonColumnInfos, (jsonColumnInfo) => jsonColumnInfo.property === actualProperty);
        if (info) {
            return {
                info,
                parts,
            };
        }
        return undefined;
    }
    static getOperator(operator, json = false) {
        switch (operator) {
            case queryTypes_1.Operator.GREATER_THAN:
                return '>';
            case queryTypes_1.Operator.GREATER_THAN_OR_EQUAL:
                return '>=';
            case queryTypes_1.Operator.LESS_THAN:
                return '<';
            case queryTypes_1.Operator.LESS_THAN_OR_EQUAL:
                return '<=';
            case queryTypes_1.Operator.NOT_EQUAL:
            case queryTypes_1.Operator.NOT:
                return '<>';
            case queryTypes_1.Operator.EQUAL:
                return '=';
            case queryTypes_1.Operator.LIKE:
            case queryTypes_1.Operator.TEXT_CONTAINS:
                return 'ilike';
            case queryTypes_1.Operator.NOT_LIKE:
            case queryTypes_1.Operator.NOT_TEXT_CONTAINS:
                return 'not ilike';
            case queryTypes_1.Operator.AND:
                return 'and';
            case queryTypes_1.Operator.OR:
                return 'or';
            case queryTypes_1.Operator.IN:
                return 'in';
            case queryTypes_1.Operator.NOT_IN:
                return 'not in';
            case queryTypes_1.Operator.BETWEEN:
                return 'between';
            case queryTypes_1.Operator.NOT_BETWEEN:
                return 'not between';
            case queryTypes_1.Operator.OVERLAP:
                if (json) {
                    return '?|';
                }
                return '&&';
            case queryTypes_1.Operator.CONTAINS:
                if (json) {
                    return '?&';
                }
                return '@>';
            default:
                throw new Error(`Unknown operator: ${operator}!`);
        }
    }
    static isJsonPropertyText(type) {
        return (type === types_1.MemberAttributeType.STRING ||
            type === types_1.MemberAttributeType.EMAIL ||
            type === types_1.MemberAttributeType.URL);
    }
    static getParamName(column, params) {
        let index = 1;
        let value = params[`${column}_${index}`];
        while (value !== undefined) {
            index++;
            value = params[`${column}_${index}`];
        }
        return `${column}_${index}`;
    }
    static getJsonParamName(column, parts, params) {
        let index = 1;
        let key;
        if (parts.length > 0) {
            key = `${column}_${parts.join('_')}`;
        }
        else {
            key = column;
        }
        let value = params[`${key}_${index}`];
        while (value !== undefined) {
            index++;
            value = params[`${key}_${index}`];
        }
        return `${key}_${index}`;
    }
    static isOperator(opOrCondition) {
        const lower = opOrCondition.toLowerCase().trim();
        return lower === 'and' || lower === 'or';
    }
}
exports.default = RawQueryParser;
//# sourceMappingURL=rawQueryParser.js.map