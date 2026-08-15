"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getArticleComments = void 0;
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
/**
 * Perform a lookup of article comments
 * @param articleId Devto article id for which to fetch the comments for
 * @returns {DevtoComment[]}
 */
const getArticleComments = async (articleId) => {
    try {
        const result = await axios_1.default.get('https://dev.to/api/comments', {
            params: {
                a_id: articleId,
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
                    return (0, exports.getArticleComments)(articleId);
                }
            }
        }
        throw err;
    }
};
exports.getArticleComments = getArticleComments;
//# sourceMappingURL=getArticleComments.js.map