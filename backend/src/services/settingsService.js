"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const settingsRepository_1 = __importDefault(require("../database/repositories/settingsRepository"));
const DEFAULT_SETTINGS = {};
class SettingsService {
    static async findOrCreateDefault(options) {
        return settingsRepository_1.default.findOrCreateDefault(DEFAULT_SETTINGS, options);
    }
    static async save(data, options) {
        const transaction = await sequelizeRepository_1.default.createTransaction(options);
        const settings = await settingsRepository_1.default.save(data, options);
        await sequelizeRepository_1.default.commitTransaction(transaction);
        return settings;
    }
    static async platformPriorityArrayExists(options) {
        const settings = await settingsRepository_1.default.findOrCreateDefault(DEFAULT_SETTINGS, options);
        return (settings.attributeSettings &&
            settings.attributeSettings.priorities &&
            settings.attributeSettings.priorities.length > 0);
    }
}
exports.default = SettingsService;
//# sourceMappingURL=settingsService.js.map