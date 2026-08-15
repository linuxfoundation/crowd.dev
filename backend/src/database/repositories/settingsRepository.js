"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const get_1 = __importDefault(require("lodash/get"));
const segmentService_1 = __importDefault(require("../../services/segmentService"));
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class SettingsRepository {
    static async findOrCreateDefault(defaults, options) {
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const [settings] = await options.database.settings.findOrCreate({
            where: { id: tenant.id, tenantId: tenant.id },
            defaults: {
                ...defaults,
                id: tenant.id,
                tenantId: tenant.id,
                createdById: currentUser ? currentUser.id : null,
            },
            transaction: sequelizeRepository_1.default.getTransaction(options),
        });
        return this._populateRelations(settings, options);
    }
    static async save(data, options) {
        var _a;
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const currentUser = sequelizeRepository_1.default.getCurrentUser(options);
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        data.backgroundImageUrl = (0, get_1.default)(data, 'backgroundImages[0].downloadUrl', null);
        data.logoUrl = (0, get_1.default)(data, 'logos[0].downloadUrl', null);
        if (typeof data.slackWebHook !== 'string' ||
            (typeof data.slackWebHook === 'string' && !((_a = data.slackWebHook) === null || _a === void 0 ? void 0 : _a.startsWith('https://')))) {
            data.slackWebHook = undefined;
        }
        const [settings] = await options.database.settings.findOrCreate({
            where: { id: tenant.id, tenantId: tenant.id },
            defaults: {
                ...data,
                id: tenant.id,
                tenantId: tenant.id,
                createdById: currentUser ? currentUser.id : null,
            },
            transaction,
        });
        await settings.update(data, {
            transaction,
        });
        return this._populateRelations(settings, options);
    }
    static async getTenantSettings(tenantId, options) {
        const transaction = sequelizeRepository_1.default.getTransaction(options);
        const settings = await options.database.settings.findOne({
            where: { tenantId },
            transaction,
        });
        return settings;
    }
    static async _populateRelations(record, options) {
        if (!record) {
            return record;
        }
        const activityTypes = segmentService_1.default.getTenantActivityTypes(options.currentSegments);
        const settings = record.get({ plain: true });
        settings.activityTypes = activityTypes;
        settings.slackWebHook = !!settings.slackWebHook;
        return settings;
    }
}
exports.default = SettingsRepository;
//# sourceMappingURL=settingsRepository.js.map