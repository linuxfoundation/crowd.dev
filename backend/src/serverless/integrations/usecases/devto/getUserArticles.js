"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAllUserArticles = exports.getUserArticles = void 0;
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
/**
 * Performs a lookup of users articles
 * @param username Dev.to user to fetch articles from
 * @param page For API pagination
 * @returns {DevtoArticle[]}
 */
const getUserArticles = async (username, page, perPage) => {
    try {
        const result = await axios_1.default.get(`https://dev.to/api/articles`, {
            params: {
                username,
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
                    return (0, exports.getUserArticles)(username, page, perPage);
                }
            }
        }
        throw err;
    }
};
exports.getUserArticles = getUserArticles;
const getAllUserArticles = async (username) => {
    let page = 1;
    const perPage = 50;
    let allArticles = [];
    let result = await (0, exports.getUserArticles)(username, page, perPage);
    while (result.length > 0) {
        allArticles = allArticles.concat(...result);
        result = await (0, exports.getUserArticles)(username, ++page, perPage);
    }
    return allArticles;
};
exports.getAllUserArticles = getAllUserArticles;
//# sourceMappingURL=getUserArticles.js.map