"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const data_access_layer_1 = require("@crowd/data-access-layer");
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
class AuditLogsService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async query(query) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        return (0, data_access_layer_1.queryAuditLogs)(qx, query);
    }
}
exports.default = AuditLogsService;
//# sourceMappingURL=auditLogsService.js.map