"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getOrganization = void 0;
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
/**
 * Performs a lookup of a Dev.to organization
 * @param organization
 * @returns {DevtoOrganization} or null if no organization found
 */
const getOrganization = async (organization, apiKey) => {
    try {
        const result = await axios_1.default.get(`https://dev.to/api/organizations/${organization}`, {
            headers: {
                Accept: 'application/vnd.forem.api-v1+json',
                'api-key': apiKey || '',
            },
        });
        return result.data;
    }
    catch (err) {
        if (err.response) {
            // rate limit?
            if (err.response.status === 429) {
                const retryAfter = err.response.headers['retry-after'];
                if (retryAfter) {
                    const retryAfterSeconds = parseInt(retryAfter, 10);
                    if (retryAfterSeconds <= 2) {
                        await (0, common_1.timeout)(1000 * retryAfterSeconds);
                        return (0, exports.getOrganization)(organization);
                    }
                }
            }
            else if (err.response.status === 404) {
                return null;
            }
        }
        throw err;
    }
};
exports.getOrganization = getOrganization;
//# sourceMappingURL=getOrganization.js.map