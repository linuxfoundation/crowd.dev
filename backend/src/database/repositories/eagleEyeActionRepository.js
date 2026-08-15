"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const common_1 = require("@crowd/common");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class EagleEyeActionRepository {
    static async createActionForContent(data, contentId, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.eagleEyeAction.create({
            ...lodash_1.default.pick(data, ['type', 'timestamp']),
            actionById: currentUser.id,
            contentId,
            tenantId: currentTenant.id,
        }, {
            transaction,
        });
        return this.findById(record.id, options);
    }
    static async removeActionFromContent(action, contentId, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const record = await options.database.eagleEyeAction.findOne({
            where: {
                contentId,
                action,
                actionById: currentUser.id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (record) {
            await record.destroy({
                transaction,
                force: true,
            });
        }
    }
    static async destroy(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.eagleEyeAction.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            transaction,
        });
        if (record) {
            await record.destroy({
                transaction,
                force: true,
            });
        }
    }
    static async findById(id, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const include = [];
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = await options.database.eagleEyeAction.findOne({
            where: {
                id,
                tenantId: currentTenant.id,
            },
            include,
            transaction,
        });
        if (!record) {
            throw new common_1.Error404();
        }
        return this._populateRelations(record);
    }
    static async create(data, options) {
        const currentTenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const record = options.database.eagleEyeContent.create({
            ...lodash_1.default.pick(data, ['type', 'timestamp']),
            tenantId: currentTenant.id,
        });
        return this.findById(record.id, options);
    }
    static async _populateRelations(record) {
        if (!record) {
            return record;
        }
        return record.get({ plain: true });
    }
}
exports.default = EagleEyeActionRepository;
//# sourceMappingURL=eagleEyeActionRepository.js.map