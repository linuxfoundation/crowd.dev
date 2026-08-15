"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const repo_1 = require("@crowd/data-access-layer/src/mergeActions/repo");
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
class MergeActionsService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async query(args) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const filters = {
            state: args.state,
            limit: args.limit,
            offset: args.offset,
        };
        const results = await (0, repo_1.findEntityMergeActions)(qx, args.entityId, args.type, filters);
        return results.map((result) => ({
            primaryId: result.primaryId,
            secondaryId: result.secondaryId,
            state: result.state,
            // derive operation type from step and if step is null, default to merge
            operationType: result.step ? MergeActionsService.getOperationType(result.step) : 'unknown',
        }));
    }
    static getOperationType(step) {
        if (step.startsWith('merge')) {
            return 'merge';
        }
        if (step.startsWith('unmerge')) {
            return 'unmerge';
        }
        throw new Error(`Unrecognized merge action step: ${step}`);
    }
}
exports.default = MergeActionsService;
//# sourceMappingURL=MergeActionsService.js.map