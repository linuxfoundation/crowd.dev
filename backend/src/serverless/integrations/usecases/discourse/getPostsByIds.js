"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDiscoursePostsByIds = void 0;
const axios_1 = __importDefault(require("axios"));
const types_1 = require("@crowd/types");
const serializeObjectToQueryString = (params) => Object.entries(params)
    .map(([key, value]) => {
    if (Array.isArray(value)) {
        return value
            .map((val) => `${encodeURIComponent(key)}[]=${encodeURIComponent(val)}`)
            .join('&');
    }
    return `${encodeURIComponent(key)}=${encodeURIComponent(value)}`;
})
    .join('&');
// this methods returns ids of posts in a topic
// then we need to parse each topic individually (can be batched)
const getDiscoursePostsByIds = async (params, input, logger) => {
    logger.info({
        message: 'Fetching posts by ids from Discourse',
        params,
        input,
    });
    const queryParameters = {
        post_ids: input.post_ids,
    };
    const queryString = serializeObjectToQueryString(queryParameters);
    const config = {
        method: 'get',
        url: `${params.forumHostname}/t/${input.topic_id}/posts.json?${queryString}`,
        headers: {
            'Api-Key': params.apiKey,
            'Api-Username': params.apiUsername,
        },
    };
    try {
        const response = await (0, axios_1.default)(config);
        return response.data;
    }
    catch (err) {
        if (err.response && err.response.status === 429) {
            // wait 5 mins
            throw new types_1.RateLimitError(5 * 60, 'discourse/getpostsbyids');
        }
        logger.error({ err, params, input }, 'Error while getting posts by ids from Discourse ');
        throw err;
    }
};
exports.getDiscoursePostsByIds = getDiscoursePostsByIds;
//# sourceMappingURL=getPostsByIds.js.map