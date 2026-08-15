"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable no-restricted-globals */
const moment_1 = __importDefault(require("moment"));
const common_1 = require("@crowd/common");
const types_1 = require("@crowd/types");
const memberAttributeSettingsRepository_1 = __importDefault(require("../database/repositories/memberAttributeSettingsRepository"));
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const camelCaseNames_1 = __importDefault(require("../utils/camelCaseNames"));
class MemberAttributeSettingsService {
    constructor(options) {
        this.options = options;
    }
    /**
     * Cherry picks attributes from predefined integration attributes.
     * @param names array of names to cherry pick
     * @param attributes list of attributes to cherry pick from
     * @returns
     */
    static pickAttributes(names, attributes) {
        return attributes.filter((i) => names.includes(i.name));
    }
    static isBoolean(value) {
        return value === true || value === 'true' || value === false || value === 'false';
    }
    static isNumber(value) {
        return ((typeof value === 'number' || (typeof value === 'string' && value.trim() !== '')) &&
            !isNaN(value));
    }
    static isEmail(value) {
        const emailRegexp = /^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$/;
        return (MemberAttributeSettingsService.isString(value) && value !== '' && value.match(emailRegexp));
    }
    static isString(value) {
        return typeof value === 'string';
    }
    static isUrl(value) {
        return MemberAttributeSettingsService.isString(value);
    }
    static isDate(value) {
        if ((0, moment_1.default)(value, moment_1.default.ISO_8601).isValid()) {
            return true;
        }
        return false;
    }
    static isMultiSelect(values, options) {
        // Type must be array
        if (!Array.isArray(values)) {
            return false;
        }
        // If empty array, it is valid
        if (values.length === 0) {
            return true;
        }
        if (!options) {
            return false;
        }
        // All values must be in options
        for (const value of values) {
            if (!options.includes(value)) {
                return false;
            }
        }
        return true;
    }
    /**
     * Checks the given attribute value against the attribute type.
     * @param value the value to be checked
     * @param type the type value will be checked against
     * @returns
     */
    static isCorrectType(value, type, inputs = {}) {
        switch (type) {
            case types_1.MemberAttributeType.BOOLEAN:
                return MemberAttributeSettingsService.isBoolean(value);
            case types_1.MemberAttributeType.STRING:
                return MemberAttributeSettingsService.isString(value);
            case types_1.MemberAttributeType.DATE:
                return MemberAttributeSettingsService.isDate(value);
            case types_1.MemberAttributeType.EMAIL:
                return MemberAttributeSettingsService.isEmail(value);
            case types_1.MemberAttributeType.URL:
                return MemberAttributeSettingsService.isUrl(value);
            case types_1.MemberAttributeType.NUMBER:
                return MemberAttributeSettingsService.isNumber(value);
            case types_1.MemberAttributeType.MULTI_SELECT:
                return MemberAttributeSettingsService.isMultiSelect(value, inputs.options);
            case types_1.MemberAttributeType.SPECIAL:
                return true;
            default:
                return false;
        }
    }
    async create(data) {
        var _a;
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            data.name = (_a = data.name) !== null && _a !== void 0 ? _a : (0, camelCaseNames_1.default)(data.label);
            const record = await memberAttributeSettingsRepository_1.default.create(data, {
                ...this.options,
                transaction,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return record;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'memberAttributeSettings');
            throw error;
        }
    }
    /**
     * Creates predefined set of attributes in one function call.
     * Useful when creating predefined platform specific attributes that come
     * from the integrations.
     * @param attributes List of attributes
     * @returns created attributes
     */
    async createPredefined(attributes, carryTransaction = null) {
        let transaction;
        if (carryTransaction) {
            transaction = carryTransaction;
        }
        else {
            transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        }
        try {
            const created = [];
            for (const attribute of attributes) {
                // check attribute already exists
                const existing = await memberAttributeSettingsRepository_1.default.findAndCountAll({ filter: { name: attribute.name } }, { ...this.options, transaction });
                if (existing.count === 0) {
                    created.push(await memberAttributeSettingsRepository_1.default.create(attribute, {
                        ...this.options,
                        transaction,
                    }));
                }
                else {
                    created.push(existing.rows[0]);
                }
            }
            if (!carryTransaction) {
                await sequelizeRepository_1.default.commitTransaction(transaction);
            }
            return created;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'memberAttributeSettings');
            throw error;
        }
    }
    async destroyAll(ids) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            for (const id of ids) {
                await memberAttributeSettingsRepository_1.default.destroy(id, {
                    ...this.options,
                    transaction,
                });
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async update(id, data) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            const attribute = await memberAttributeSettingsRepository_1.default.findById(id, {
                ...this.options,
                transaction,
            });
            // we're not allowing updating attribute type to some other value
            if (data.type && attribute.type !== data.type) {
                throw new common_1.Error400(this.options.language, 'settings.memberAttributes.errors.typesNotMatching');
            }
            // readonly canDelete field can't be updated to some other value
            if ((data.canDelete === true || data.canDelete === false) &&
                attribute.canDelete !== data.canDelete) {
                throw new common_1.Error400(this.options.language, 'settings.memberAttributes.errors.canDeleteReadonly');
            }
            // not allowing updating name field as well, delete just in case if name != data.name
            delete data.name;
            const record = await memberAttributeSettingsRepository_1.default.update(id, data, {
                ...this.options,
                transaction,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return record;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'memberAttributeSettings');
            throw error;
        }
    }
    async findAndCountAll(args) {
        return memberAttributeSettingsRepository_1.default.findAndCountAll(args, this.options);
    }
    async findById(id) {
        return memberAttributeSettingsRepository_1.default.findById(id, this.options);
    }
}
exports.default = MemberAttributeSettingsService;
//# sourceMappingURL=memberAttributeSettingsService.js.map