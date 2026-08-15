"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const data_access_layer_1 = require("@crowd/data-access-layer");
const segments_1 = require("@crowd/data-access-layer/src/segments");
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const sequelizeRepository_1 = __importDefault(require("@/database/repositories/sequelizeRepository"));
const queueService_1 = require("@/serverless/utils/queueService");
const activityRepository_1 = __importDefault(require("../database/repositories/activityRepository"));
const memberTypes_1 = require("../database/repositories/types/memberTypes");
const segmentService_1 = __importDefault(require("./segmentService"));
class ActivityService extends logging_1.LoggerBase {
    constructor(options) {
        super(options.log);
        this.options = options;
    }
    async createWithMember(data) {
        const logger = this.options.log;
        const dataSinkWorkerEmitter = await (0, queueService_1.getDataSinkWorkerEmitter)();
        try {
            data.member.username = (0, memberTypes_1.mapUsernameToIdentities)(data.member.username, data.platform);
            if (!data.username) {
                data.username = data.member.username[data.platform][0].value;
            }
            logger.trace({ type: data.type, platform: data.platform, username: data.username }, 'Processing activity with member!');
            data.member.identities = ActivityService.processMemberIdentities(data.member, data.platform);
            // prepare objectMember for dataSinkWorker
            if (data.objectMember) {
                data.objectMember.username = (0, memberTypes_1.mapUsernameToIdentities)(data.objectMember.username, data.platform);
                if (!data.objectMember.username[data.platform]) {
                    throw new Error(`objectMember username for ${data.platform} is missing!`);
                }
                data.objectMemberUsername = data.objectMember.username[data.platform][0].value;
                data.objectMember.identities = ActivityService.processMemberIdentities(data.objectMember, data.platform);
            }
            if (data.member.organizations) {
                data.member.organizations.forEach((org) => {
                    org.identities = [
                        {
                            name: org.name || org.website,
                            platform: data.platform,
                        },
                    ];
                });
            }
            const resultId = await activityRepository_1.default.createResults({
                type: types_1.IntegrationResultType.ACTIVITY,
                data,
            }, this.options);
            logger.trace({ type: data.type, platform: data.platform, username: data.username, processedData: data }, 'Sending activity with member to data-sink-worker!');
            await dataSinkWorkerEmitter.triggerResultProcessing(resultId, resultId, true);
        }
        catch (error) {
            this.log.error(error, 'Error during activity create with member!');
            throw error;
        }
    }
    async findActivityTypes() {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(this.options);
        const subprojects = await (0, segments_1.getSegmentSubprojects)(qx, currentSegments);
        return segmentService_1.default.getTenantActivityTypes(subprojects);
    }
    async findActivityChannels() {
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(this.options);
        const subprojectIds = await (0, segments_1.getSegmentSubprojectIds)(qx, currentSegments);
        if (subprojectIds.length === 0) {
            return {};
        }
        return segmentService_1.default.getTenantActivityChannels(subprojectIds, this.options);
    }
    async query(data) {
        var _a;
        const filter = data.filter;
        const orderBy = Array.isArray(data.orderBy) ? data.orderBy : [data.orderBy];
        const limit = data.limit;
        const offset = data.offset;
        const countOnly = (_a = data.countOnly) !== null && _a !== void 0 ? _a : false;
        const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
        const currentSegments = sequelizeRepository_1.default.getSegmentIds(this.options);
        const subprojects = await (0, segments_1.getSegmentSubprojects)(qx, currentSegments);
        if (subprojects.length === 0) {
            return {
                count: 0,
                rows: [],
                limit,
                offset,
            };
        }
        const activitiyTypes = segmentService_1.default.getTenantActivityTypes(subprojects);
        const page = await (0, data_access_layer_1.queryActivities)({
            segmentIds: subprojects.map((s) => s.id),
            filter,
            orderBy,
            limit,
            offset,
            countOnly,
        }, qx, activitiyTypes);
        return page;
    }
    static processMemberIdentities(member, platform) {
        const identities = [];
        if (member.username) {
            Object.keys(member.username).forEach((platform) => {
                identities.push({
                    platform,
                    value: member.username[platform][0].value,
                    type: member.username[platform][0].type,
                    verified: true,
                });
            });
        }
        if (member.emails) {
            member.emails.forEach((email) => {
                identities.push({
                    platform,
                    value: email,
                    type: 'email',
                    verified: true,
                });
            });
        }
        return identities;
    }
}
exports.default = ActivityService;
//# sourceMappingURL=activityService.js.map