"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = __importStar(require("sequelize"));
const conf_1 = require("../../conf");
class SequelizeArrayUtils {
    // MySQL doesn't have Array Field
    static get DataType() {
        return conf_1.DB_CONFIG.dialect === 'mysql' ? sequelize_1.DataTypes.JSON : sequelize_1.DataTypes.ARRAY(sequelize_1.DataTypes.TEXT);
    }
    static filter(tableName, fieldName, filterValue) {
        const filterValueAsArray = Array.isArray(filterValue) ? filterValue : [filterValue];
        if (conf_1.DB_CONFIG.dialect === 'mysql') {
            return {
                [sequelize_1.default.Op.and]: filterValueAsArray.map((filterValue) => arrayContainsForMySQL(tableName, fieldName, filterValue)),
            };
        }
        return {
            [fieldName]: {
                [sequelize_1.default.Op.contains]: filterValueAsArray,
            },
        };
    }
}
exports.default = SequelizeArrayUtils;
function arrayContainsForMySQL(model, column, value) {
    return sequelize_1.default.fn('JSON_CONTAINS', sequelize_1.default.col(`${model}.${column}`), `"${value}"`);
}
//# sourceMappingURL=sequelizeArrayUtils.js.map