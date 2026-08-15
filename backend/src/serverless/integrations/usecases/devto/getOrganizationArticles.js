"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAllOrganizationArticles = exports.getOrganizationArticles = void 0;
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
/**
 * Performs a lookup of organization articles
 * @param organizationName Organization name to fetch articles from
 * @param page For API pagination
 * @param perPage For API pagination
 * @returns {DevtoArticle[]}
 */
const getOrganizationArticles = async (organizationName, page, perPage) => {
    try {
        const result = await axios_1.default.get(`https://dev.to/api/organizations/${encodeURIComponent(organizationName)}/articles`, {
            params: {
                page,
                per_page: perPage,
            },
        });
        return result.data;
    }
    catch (err) {
        // rate limit?
        if (err.response.status === 429) {
            const retryAfter = err.response.headers['retry-after'];
            if (retryAfter) {
                const retryAfterSeconds = parseInt(retryAfter, 10);
                if (retryAfterSeconds <= 2) {
                    await (0, common_1.timeout)(1000 * retryAfterSeconds);
                    return (0, exports.getOrganizationArticles)(organizationName, page, perPage);
                }
            }
        }
        throw err;
    }
};
exports.getOrganizationArticles = getOrganizationArticles;
const getAllOrganizationArticles = async (organizationName) => {
    let page = 1;
    const perPage = 50;
    let allArticles = [];
    let result = await (0, exports.getOrganizationArticles)(organizationName, page, perPage);
    while (result.length > 0) {
        allArticles = allArticles.concat(...result);
        result = await (0, exports.getOrganizationArticles)(organizationName, ++page, perPage);
    }
    return allArticles;
};
exports.getAllOrganizationArticles = getAllOrganizationArticles;
//# sourceMappingURL=getOrganizationArticles.js.map