"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const sequelizeRepository_1 = __importDefault(require("./sequelizeRepository"));
class IntegrationProgressRepository {
    static createPayloadWithActivityType(activityTypes, repos, segments = []) {
        return {
            filter: {
                and: [
                    { platform: { in: ['github'] } },
                    { or: repos.map((repo) => ({ channel: { eq: repo.url } })) },
                    { type: { in: activityTypes } },
                ],
            },
            segmentIds: segments,
        };
    }
    static async getPendingStreamsCount(integrationId, options) {
        const transaction = options.transaction;
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const lastRunId = await IntegrationProgressRepository.getLastRunId(integrationId, options);
        if (!lastRunId) {
            return 0;
        }
        const result = await seq.query(`
            select count(*) as "total"
            from integration.streams
            where "integrationId" = :integrationId
            and "runId" = :lastRunId
            and "state" = 'pending'
            `, {
            replacements: {
                integrationId,
                lastRunId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return result[0].total;
    }
    static async getLastRunId(integrationId, options) {
        const transaction = options.transaction;
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const result = await seq.query(`
        select id
        from integration.runs
        where "integrationId" = :integrationId
        order by "createdAt" desc
        limit 1
        `, {
            replacements: {
                integrationId,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        if (result.length === 0) {
            return null;
        }
        return result[0].id;
    }
    static async getDbStatsForGithub() {
        // const tb = new TinybirdClient()
        // const promises: Promise<{ data: Counter }>[] = [
        //   queryActivitiesCounter(
        //     IntegrationProgressRepository.createPayloadWithActivityType(['star'], repos, segments),
        //     tb,
        //   ),
        //   queryActivitiesCounter(
        //     IntegrationProgressRepository.createPayloadWithActivityType(['unstar'], repos, segments),
        //     tb,
        //   ),
        //   queryActivitiesCounter(
        //     {
        //       ...IntegrationProgressRepository.createPayloadWithActivityType(['fork'], repos, segments),
        //       indirectFork: 1,
        //     },
        //     tb,
        //   ),
        //   queryActivitiesCounter(
        //     IntegrationProgressRepository.createPayloadWithActivityType(
        //       ['issues-opened'],
        //       repos,
        //       segments,
        //     ),
        //     tb,
        //   ),
        //   queryActivitiesCounter(
        //     IntegrationProgressRepository.createPayloadWithActivityType(
        //       ['pull_request-opened'],
        //       repos,
        //       segments,
        //     ),
        //     tb,
        //   ),
        // ]
        // const result = await Promise.all(promises)
        return {
            // stars: (result[0]?.data?.[0]?.count ?? 0) - (result[1]?.data?.[0]?.count ?? 0),
            // forks: result[2]?.data?.[0]?.count ?? 0,
            // totalIssues: result[3]?.data?.[0]?.count ?? 0,
            // totalPRs: result[4]?.data?.[0]?.count ?? 0,
            stars: 0,
            forks: 0,
            totalIssues: 0,
            totalPRs: 0,
        };
    }
    static async getAllIntegrationsInProgressForSegment(options) {
        const transaction = options.transaction;
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const segment = sequelizeRepository_1.default.getStrictlySingleActiveSegment(options);
        const result = await seq.query(`
      select id
      from integrations
      where 
        "status" = 'in-progress'
        and "segmentId" = :segmentId
        and "deletedAt" is null
      `, {
            replacements: {
                segmentId: segment.id,
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return result.map((r) => r.id);
    }
    static async getAllIntegrationsInProgressForMultipleSegments(options) {
        const transaction = options.transaction;
        const seq = sequelizeRepository_1.default.getSequelize(options);
        const segments = sequelizeRepository_1.default.getCurrentSegments(options);
        const result = await seq.query(`
      select id
      from integrations
      where 
        "status" = 'in-progress'
        and "segmentId" in (:segmentIds)
        and "deletedAt" is null
      `, {
            replacements: {
                segmentIds: segments.map((s) => s.id),
            },
            type: sequelize_1.QueryTypes.SELECT,
            transaction,
        });
        return result.map((r) => r.id);
    }
}
exports.default = IntegrationProgressRepository;
//# sourceMappingURL=integrationProgressRepository.js.map