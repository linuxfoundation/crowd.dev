"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const data_quality_1 = require("@crowd/data-access-layer/src/data-quality");
const logging_1 = require("@crowd/logging");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const data_quality_filters_1 = require("@/types/data-quality/data-quality-filters");
class DataQualityService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    /**
     * Finds issues related to member data quality based on the specified type.
     *
     * @param {IDataQualityParams} params - The parameters for finding member issues, including the type of issue, limit, and offset.
     * @param {string} segmentId - The ID of the segment where the members belong.
     * @return {Promise<Array>} A promise that resolves to an array of members with the specified data quality issues.
     */
    async findMemberIssues(params, segmentId) {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const limit = params.limit || 10;
        const offset = params.offset || 0;
        switch (params.type) {
            case data_quality_filters_1.IDataQualityType.NO_WORK_EXPERIENCE:
                return (0, data_quality_1.fetchMembersWithoutWorkExperience)(qx, limit, offset, segmentId);
            case data_quality_filters_1.IDataQualityType.TOO_MANY_IDENTITIES:
                return (0, data_quality_1.fetchMembersWithTooManyIdentities)(qx, 30, limit, offset, segmentId);
            case data_quality_filters_1.IDataQualityType.TOO_MANY_IDENTITIES_PER_PLATFORM:
                return (0, data_quality_1.fetchMembersWithTooManyIdentitiesPerPlatform)(qx, 1, limit, offset, segmentId);
            case data_quality_filters_1.IDataQualityType.TOO_MANY_EMAILS:
                return (0, data_quality_1.fetchMembersWithTooManyEmails)(qx, 5, limit, offset, segmentId);
            case data_quality_filters_1.IDataQualityType.WORK_EXPERIENCE_MISSING_INFO:
                return (0, data_quality_1.fetchMembersWithMissingInfoOnWorkExperience)(qx, limit, offset, segmentId);
            case data_quality_filters_1.IDataQualityType.WORK_EXPERIENCE_MISSING_PERIOD:
                return (0, data_quality_1.fetchMembersWithMissingPeriodOnWorkExperience)(qx, limit, offset, segmentId);
            case data_quality_filters_1.IDataQualityType.CONFLICTING_WORK_EXPERIENCE:
                return (0, data_quality_1.fetchMembersWithConflictingWorkExperiences)(qx, limit, offset, segmentId);
            default:
                throw new Error(`Unsupported data quality filter type: ${params.type}`);
        }
    }
    // TODO: Implement this method when there are checks available
    // eslint-disable-next-line class-methods-use-this
    async findOrganizationIssues() {
        return Promise.resolve([]);
    }
}
exports.default = DataQualityService;
//# sourceMappingURL=dataQualityService.js.map