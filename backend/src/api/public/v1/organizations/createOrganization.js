"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createOrganization = createOrganization;
const zod_1 = require("zod");
const audit_logs_1 = require("@crowd/audit-logs");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const types_1 = require("@crowd/types");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const bodySchema = zod_1.z.object({
    name: zod_1.z.string().trim().min(1),
    domain: zod_1.z.string().trim().min(1),
    source: zod_1.z.string().trim().min(1),
    logo: zod_1.z.string().trim().min(1).optional(),
});
async function createOrganization(req, res) {
    const { name, domain: rawDomain, source, logo } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const domain = (0, common_1.normalizeHostname)(rawDomain, false);
    if (!domain) {
        throw new common_1.BadRequestError(`Invalid domain: ${rawDomain}`);
    }
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const organizationId = await qx.tx(async (tx) => {
        const orgSource = types_1.OrganizationAttributeSource.LFX_SERVE;
        const organizationId = await (0, data_access_layer_1.findOrCreateOrganization)(tx, orgSource, {
            displayName: name,
            logo,
            identities: [
                {
                    value: domain,
                    type: types_1.OrganizationIdentityType.PRIMARY_DOMAIN,
                    verified: true,
                    platform: orgSource,
                    source,
                },
            ],
        });
        if (!organizationId) {
            throw new common_1.InternalError('Failed to create organization');
        }
        await (0, audit_logs_1.captureApiChange)(req, (0, audit_logs_1.organizationCreateAction)(organizationId, async (captureState) => {
            captureState({
                id: organizationId,
                displayName: name,
                identities: [
                    {
                        value: domain,
                        type: types_1.OrganizationIdentityType.PRIMARY_DOMAIN,
                    },
                ],
            });
        }));
        return organizationId;
    });
    (0, api_1.created)(res, { id: organizationId, name, logo, domain });
}
//# sourceMappingURL=createOrganization.js.map