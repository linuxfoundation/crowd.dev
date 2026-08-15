"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const types_1 = require("@crowd/types");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class ActivityRepository {
    static async createResults(result, options) {
        const tenant = sequelizeRepository_1.default.getCurrentTenant(options);
        const segment = sequelizeRepository_1.default.getStrictlySingleActiveSegment(options);
        const seq = sequelizeRepository_1.default.getSequelize(options);
        result.segmentId = segment.id;
        const results = await seq.query(`
      insert into integration.results(state, data, "tenantId")
      values(:state, :data, :tenantId)
      returning id;
      `, {
            replacements: {
                tenantId: tenant.id,
                state: types_1.IntegrationResultState.PENDING,
                data: JSON.stringify(result),
            },
            type: sequelize_1.QueryTypes.INSERT,
        });
        return results[0][0].id;
    }
}
exports.default = ActivityRepository;
//# sourceMappingURL=activityRepository.js.map