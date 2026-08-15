"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CROWD_ANALYTICS_PLATORM_NAME = void 0;
exports.default = addProductData;
const axios_1 = __importDefault(require("axios"));
const logging_1 = require("@crowd/logging");
const conf_1 = require("../conf");
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const tenantRepository_1 = __importDefault(require("../database/repositories/tenantRepository"));
const userRepository_1 = __importDefault(require("../database/repositories/userRepository"));
const IS_CROWD_ANALYTICS_ENABLED = conf_1.CROWD_ANALYTICS_CONFIG.isEnabled === 'true';
const CROWD_ANALYTICS_TENANT_ID = conf_1.CROWD_ANALYTICS_CONFIG.tenantId;
const CROWD_ANALYTICS_BASE_URL = conf_1.CROWD_ANALYTICS_CONFIG.baseUrl;
const CROWD_ANALYTICS_TOKEN = conf_1.CROWD_ANALYTICS_CONFIG.apiToken;
exports.CROWD_ANALYTICS_PLATORM_NAME = 'crowd.dev-analytics';
const log = (0, logging_1.getServiceChildLogger)('segment');
const expandAttributes = (attributes) => {
    const obj = {};
    Object.keys(attributes).forEach((key) => {
        obj[key.toLowerCase()] = {
            [exports.CROWD_ANALYTICS_PLATORM_NAME]: attributes[key],
        };
    });
    return obj;
};
async function addProductData(data) {
    var _a, _b, _c, _d;
    if (!IS_CROWD_ANALYTICS_ENABLED) {
        return;
    }
    if (!CROWD_ANALYTICS_TENANT_ID) {
        return;
    }
    if (!CROWD_ANALYTICS_BASE_URL) {
        return;
    }
    if (!CROWD_ANALYTICS_TOKEN) {
        return;
    }
    if (!(data === null || data === void 0 ? void 0 : data.userId)) {
        // we can't send data without a user id
        return;
    }
    if (!(data === null || data === void 0 ? void 0 : data.tenantId)) {
        // we can't send data without a tenant id
        return;
    }
    try {
        const repositoryOptions = await sequelizeRepository_1.default.getDefaultIRepositoryOptions();
        const user = await userRepository_1.default.findById(data.userId, repositoryOptions);
        // this is an array of one tenant
        const tenant = await tenantRepository_1.default.getTenantInfo(data.tenantId, repositoryOptions);
        const timestamp = data.timestamp || new Date().toISOString();
        const obj = {
            member: {
                username: {
                    [exports.CROWD_ANALYTICS_PLATORM_NAME]: user.email,
                },
                emails: [user.email],
                displayName: user.fullName,
                attributes: {
                    ...expandAttributes({
                        email: user.email,
                        createdAnAccount: true,
                        firstName: user.firstName,
                        lastName: user.lastName,
                        plan: (_a = tenant[0]) === null || _a === void 0 ? void 0 : _a.plan,
                        isTrialPlan: (_b = tenant[0]) === null || _b === void 0 ? void 0 : _b.isTrialPlan,
                        trialEndsAt: (_c = tenant[0]) === null || _c === void 0 ? void 0 : _c.trialEndsAt,
                    }),
                },
                organizations: [
                    {
                        name: (_d = tenant[0]) === null || _d === void 0 ? void 0 : _d.name,
                    },
                ],
            },
            type: data.event,
            timestamp,
            platform: exports.CROWD_ANALYTICS_PLATORM_NAME,
            sourceId: `${data.userId}-${timestamp}-${data.event}`,
        };
        const endpoint = `${CROWD_ANALYTICS_BASE_URL}/tenant/${CROWD_ANALYTICS_TENANT_ID}/activity/with-member`;
        await axios_1.default.post(endpoint, obj, {
            headers: {
                Authorization: `Bearer ${CROWD_ANALYTICS_TOKEN}`,
            },
        });
    }
    catch (error) {
        log.error(error, { data }, 'ERROR: Could not send the following payload to Crowd Analytics');
    }
}
//# sourceMappingURL=addProductDataToCrowdTenant.js.map