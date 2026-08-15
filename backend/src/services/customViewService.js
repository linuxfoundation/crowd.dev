"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const logging_1 = require("@crowd/logging");
const customViewRepository_1 = __importDefault(require("../database/repositories/customViewRepository"));
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
class CustomViewService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async create(data) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            const record = await customViewRepository_1.default.create(data, {
                ...this.options,
                transaction,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return record;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'customView');
            throw error;
        }
    }
    async update(id, data) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            const record = await customViewRepository_1.default.update(id, data, {
                ...this.options,
                transaction,
            });
            await sequelizeRepository_1.default.commitTransaction(transaction);
            return record;
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            sequelizeRepository_1.default.handleUniqueFieldError(error, this.options.language, 'customView');
            throw error;
        }
    }
    async destroyAll(ids) {
        const transaction = await sequelizeRepository_1.default.createTransaction(this.options);
        try {
            for (const id of ids) {
                await customViewRepository_1.default.destroy(id, this.options);
            }
            await sequelizeRepository_1.default.commitTransaction(transaction);
        }
        catch (error) {
            await sequelizeRepository_1.default.rollbackTransaction(transaction);
            throw error;
        }
    }
    async findById(id) {
        return customViewRepository_1.default.findById(id, this.options);
    }
    async findAll(args) {
        return customViewRepository_1.default.findAll(args, this.options);
    }
}
exports.default = CustomViewService;
//# sourceMappingURL=customViewService.js.map