"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dashboards_1 = require("@crowd/data-access-layer/src/dashboards");
const redis_1 = require("@crowd/redis");
const types_1 = require("@crowd/types");
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
class DashboardService {
    constructor(options) {
        this.options = options;
    }
    async get(params) {
        var _a;
        if (!params.timeframe) {
            throw new Error(`Timeframe is required!`);
        }
        if (!Object.values(types_1.DashboardTimeframe).includes(params.timeframe)) {
            throw new Error(`Unsupported timeframe ${params.timeframe}!`);
        }
        if (!params.segment) {
            params.segment = (_a = this.options.currentSegments[0]) === null || _a === void 0 ? void 0 : _a.id;
        }
        if (!params.segment) {
            throw new Error('Valid segment ID is required');
        }
        let key = `${params.segment}:${params.timeframe}`;
        if (params.platform) {
            key += `:${params.platform}`;
        }
        const cache = new redis_1.RedisCache('dashboard-cache', this.options.redis, this.options.log);
        const data = await cache.get(key);
        if (!data) {
            return {
                newMembers: {
                    total: 0,
                    previousPeriodTotal: 0,
                    timeseries: null,
                },
                activeMembers: {
                    total: 0,
                    previousPeriodTotal: 0,
                    timeseries: null,
                },
                newOrganizations: {
                    total: 0,
                    previousPeriodTotal: 0,
                    timeseries: null,
                },
                activeOrganizations: {
                    total: 0,
                    previousPeriodTotal: 0,
                    timeseries: null,
                },
                activity: {
                    total: 0,
                    previousPeriodTotal: 0,
                    timeseries: null,
                    bySentimentMood: null,
                    byTypeAndPlatform: null,
                },
            };
        }
        return JSON.parse(data);
    }
    async getMetrics(params) {
        try {
            if (!params.segment) {
                this.options.log.warn('No segment ID provided for metrics query');
            }
            const qx = sequelizeRepository_1.default.getQueryExecutor(this.options);
            const metrics = await (0, dashboards_1.getMetrics)(qx, params.segment);
            return metrics;
        }
        catch (error) {
            this.options.log.error('Failed to fetch dashboard metrics', { error, params });
            throw new Error('Unable to fetch dashboard metrics');
        }
    }
}
exports.default = DashboardService;
//# sourceMappingURL=dashboardService.js.map