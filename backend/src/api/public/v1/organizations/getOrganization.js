"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getOrganization = getOrganization;
const zod_1 = require("zod");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const querySchema = zod_1.z
    .object({
    name: zod_1.z.string().trim().min(1).optional(),
    domain: zod_1.z.string().trim().min(1).optional(),
})
    .refine((data) => data.name || data.domain, {
    message: 'Either name or domain must be provided',
});
async function getOrganization(req, res) {
    const { name, domain: rawDomain } = (0, validation_1.validateOrThrow)(querySchema, req.query);
    const domain = rawDomain ? (0, common_1.normalizeHostname)(rawDomain, false) : undefined;
    if (rawDomain && !domain) {
        throw new common_1.BadRequestError(`Invalid domain: ${rawDomain}`);
    }
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const organization = await (0, data_access_layer_1.findOrganizationByNameOrDomain)(qx, {
        name,
        domain,
    });
    if (!organization) {
        throw new common_1.NotFoundError('Organization not found');
    }
    const { logo, ...rest } = organization;
    (0, api_1.ok)(res, { ...rest, ...(logo ? { logo } : {}) });
}
//# sourceMappingURL=getOrganization.js.map