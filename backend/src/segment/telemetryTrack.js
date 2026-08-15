"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = track;
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const conf_1 = require("../conf");
const sequelizeRepository_1 = __importDefault(require("../database/repositories/sequelizeRepository"));
const addProductDataToCrowdTenant_1 = require("./addProductDataToCrowdTenant");
const trackHelper_1 = __importDefault(require("./trackHelper"));
const log = (0, logging_1.getServiceChildLogger)('telemetryTrack');
function track(event, properties, options, userId = false, timestamp = false) {
    const email = sequelizeRepository_1.default.getCurrentUser({
        ...options,
    }).email;
    if (!conf_1.IS_TEST_ENV &&
        conf_1.SEGMENT_CONFIG.writeKey &&
        // This is only for events in the self-hosted version. Hosted has more telemetry.
        conf_1.API_CONFIG.edition === types_1.Edition.COMMUNITY &&
        !email.includes('crowd.dev')) {
        if (properties &&
            (properties === null || properties === void 0 ? void 0 : properties.platform) &&
            (properties === null || properties === void 0 ? void 0 : properties.platform) === addProductDataToCrowdTenant_1.CROWD_ANALYTICS_PLATORM_NAME) {
            // no need to track crowd analytics events in segment
            // and this is also to ensure we don't get into an infinite loop
            return;
        }
        const Analytics = require('analytics-node');
        const analytics = new Analytics(conf_1.SEGMENT_CONFIG.writeKey);
        const { userIdOut, tenantIdOut } = (0, trackHelper_1.default)(userId, options);
        const payload = {
            userId: userIdOut,
            event,
            properties,
            context: {
                groupId: tenantIdOut,
            },
            ...(timestamp && { timestamp }),
        };
        try {
            if (event === 'Conversation created') {
                log.trace('Added conversation');
            }
            analytics.track(payload);
        }
        catch (error) {
            log.error(error, { payload }, 'ERROR: Could not send the following payload to Segment');
        }
    }
}
//# sourceMappingURL=telemetryTrack.js.map