"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = __importDefault(require("sequelize"));
const validator_1 = __importDefault(require("validator"));
const common_1 = require("@crowd/common");
/**
 * Utilities to use on Sequelize queries.
 */
class SequelizeFilterUtils {
    /**
     * If you pass an invalid uuid to a query, it throws an exception.
     * To hack this behaviour, if the uuid is invalid, it creates a new one,
     * that won't match any of the database.
     * If the uuid is invalid, brings no results.
     */
    static uuid(value) {
        let id = value;
        // If ID is invalid, sequelize throws an error.
        // For that not to happen, if the UUID is invalid, it sets
        // some random uuid
        if (!validator_1.default.isUUID(id)) {
            id = (0, common_1.generateUUIDv4)();
        }
        return id;
    }
    /**
     * Creates an ilike condition.
     */
    static ilikeIncludes(model, column, value) {
        return sequelize_1.default.where(sequelize_1.default.col(`${model}.${column}`), {
            [sequelize_1.default.Op.iLike]: `%${value}%`.toLowerCase(),
        });
    }
    static ilikeIncludesCaseSensitive(model, column, value) {
        return sequelize_1.default.where(sequelize_1.default.col(`${model}.${column}`), {
            [sequelize_1.default.Op.like]: `%${value}%`,
        });
    }
    static ilikeExact(model, column, value) {
        return sequelize_1.default.where(sequelize_1.default.col(`${model}.${column}`), {
            [sequelize_1.default.Op.like]: (value || '').toLowerCase(),
        });
    }
    static jsonbILikeIncludes(model, column, value) {
        return sequelize_1.default.where(sequelize_1.default.literal(`CAST("${model}"."${column}" AS TEXT)`), {
            [sequelize_1.default.Op.like]: `%${value}%`.toLowerCase(),
        });
    }
    static customOrderByIfExists(field, orderBy) {
        if (orderBy.includes(field)) {
            return [sequelize_1.default.literal(`"${field}"`), orderBy.split('_')[1]];
        }
        return [];
    }
    static getFieldLiteral(field, model) {
        return sequelize_1.default.col(`"${model}"."${field}"`);
    }
    static getLiteralProjections(fields, model) {
        return fields.reduce((acc, field) => {
            acc.push([SequelizeFilterUtils.getFieldLiteral(field, model), field]);
            return acc;
        }, []);
    }
    static getLiteralProjectionsOfModel(model, models, modelAlias = null) {
        return SequelizeFilterUtils.getLiteralProjections(Object.keys(models[model].rawAttributes), modelAlias !== null && modelAlias !== void 0 ? modelAlias : model);
    }
    static getNativeTableFieldAggregations(fields, model) {
        return fields.reduce((acc, field) => {
            acc[field] = SequelizeFilterUtils.getFieldLiteral(field, model);
            return acc;
        }, {});
    }
}
exports.default = SequelizeFilterUtils;
//# sourceMappingURL=sequelizeFilterUtils.js.map