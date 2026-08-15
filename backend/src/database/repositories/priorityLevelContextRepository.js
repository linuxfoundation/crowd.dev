"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PriorityLevelContextRepository = void 0;
const sequelize_1 = require("sequelize");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class PriorityLevelContextRepository {
    constructor(options) {
        this.options = options;
    }
    async loadPriorityLevelContext(tenantId) {
        const seq = sequelizeRepository_1.default.getSequelize(this.options);
        const results = await seq.query(`select plan, "priorityLevel" as "dbPriority" from tenants where id = :tenantId`, {
            replacements: {
                tenantId,
            },
            type: sequelize_1.QueryTypes.SELECT,
        });
        if (results.length === 1) {
            return results[0];
        }
        throw new Error(`Tenant not found: ${tenantId}!`);
    }
}
exports.PriorityLevelContextRepository = PriorityLevelContextRepository;
//# sourceMappingURL=priorityLevelContextRepository.js.map