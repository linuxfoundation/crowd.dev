"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = getUserContext;
const segmentRepository_1 = __importDefault(require("../repositories/segmentRepository"));
const sequelizeRepository_1 = __importDefault(require("../repositories/sequelizeRepository"));
const tenantRepository_1 = __importDefault(require("../repositories/tenantRepository"));
const userRepository_1 = __importDefault(require("../repositories/userRepository"));
/**
 * Gets the IRepositoryOptions for given tenantId
 * Tries to inject user context of the given tenant as well (if tenant is associated with a user)
 * Useful when working outside express contexts(serverless)
 * @param tenantId
 * @returns IRepositoryOptions injected with currentTenant and currentUser
 */
async function getUserContext(tenantId, userId, segmentIds) {
    const options = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
    const tenant = await tenantRepository_1.default.findById(tenantId, {
        ...options,
    });
    options.currentTenant = tenant;
    let user = null;
    if (userId) {
        user = await userRepository_1.default.findById(userId, {
            ...options,
            currentTenant: tenant,
            bypassPermissionValidation: true,
        });
    }
    else {
        const tenantUsers = await tenant.getUsers();
        if (tenantUsers.length > 0) {
            user = await userRepository_1.default.findById(tenantUsers[0].userId, {
                ...options,
                currentTenant: tenant,
                bypassPermissionValidation: true,
            });
        }
    }
    const segmentRepository = new segmentRepository_1.default(options);
    const segments = segmentIds && segmentIds.length
        ? await segmentRepository.findInIds(segmentIds)
        : (await segmentRepository.querySubprojects({ limit: 1, offset: 0 })).rows;
    // Inject user and tenant to IRepositoryOptions
    return sequelizeRepository_1.default.getDefaultIRepositoryOptions(user, tenant, segments);
}
//# sourceMappingURL=getUserContext.js.map