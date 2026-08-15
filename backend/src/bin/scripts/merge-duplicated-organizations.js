"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const sequelize_1 = require("sequelize");
const logging_1 = require("@crowd/logging");
const segmentRepository_1 = __importDefault(require("@/database/repositories/segmentRepository"));
const sequelizeRepository_1 = __importDefault(require("../../database/repositories/sequelizeRepository"));
const organizationService_1 = __importDefault(require("../../services/organizationService"));
/* eslint-disable no-continue */
/* eslint-disable @typescript-eslint/no-loop-func */
const log = (0, logging_1.getServiceLogger)();
async function mergeOrganizationsWithSameWebsite() {
    const dbOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const BATCH_SIZE = 500;
    let offset;
    let mergeableOrganizations;
    const seq = sequelizeRepository_1.default.getSequelize(dbOptions);
    log.info('Querying database for organizations with same website in a tenant..');
    do {
        offset = mergeableOrganizations ? offset + BATCH_SIZE : 0;
        mergeableOrganizations = await seq.query(`
        select 
          array_agg(o.id order by o."createdAt" asc) as "organizationIds", 
          o.website, 
          o."tenantId" 
        from organizations o
        where o.website is not null and o."deletedAt" is null
        group by o."tenantId", o.website
        having count(o.id) > 1
        limit ${BATCH_SIZE}
        offset ${offset};`, {
            type: sequelize_1.QueryTypes.SELECT,
        });
        log.info(`Found ${mergeableOrganizations.length} mergeable organizations.`);
        for (const orgInfo of mergeableOrganizations) {
            const segmentRepository = new segmentRepository_1.default({
                ...dbOptions,
                currentTenant: {
                    id: orgInfo.tenantId,
                },
            });
            const segments = (await segmentRepository.querySubprojects({ limit: null, offset: 0 })).rows;
            const service = new organizationService_1.default({
                ...dbOptions,
                currentTenant: {
                    id: orgInfo.tenantId,
                },
                currentSegments: segments,
            });
            const primaryOrganizationId = orgInfo.organizationIds.shift();
            for (const orgId of orgInfo.organizationIds) {
                log.info(`Merging organization ${orgId} into ${primaryOrganizationId}!`);
                await service.mergeSync(primaryOrganizationId, orgId, null);
            }
        }
    } while (mergeableOrganizations.length > 0);
}
setImmediate(async () => {
    log.info('Starting merging organizations with same website!');
    await mergeOrganizationsWithSameWebsite();
});
//# sourceMappingURL=merge-duplicated-organizations.js.map