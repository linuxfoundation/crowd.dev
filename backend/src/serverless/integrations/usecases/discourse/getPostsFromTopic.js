"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDiscoursePostsFromTopic = void 0;
const axios_1 = __importDefault(require("axios"));
const types_1 = require("@crowd/types");
// this methods returns ids of posts in a topic
// then we need to parse each topic individually (can be batched)
const getDiscoursePostsFromTopic = async (params, input, logger) => {
    logger.info({
        message: 'Fetching posts from topic from Discourse',
        params,
        input,
    });
    const config = {
        method: 'get',
        url: `${params.forumHostname}/t/${input.topic_slug}/${input.topic_id}.json`,
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
            throw new types_1.RateLimitError(5 * 60, 'discourse/getpostsfromtopic');
        }
        logger.error({ err, params, input }, 'Error while getting posts from topic from Discourse ');
        throw err;
    }
};
exports.getDiscoursePostsFromTopic = getDiscoursePostsFromTopic;
//# sourceMappingURL=getPostsFromTopic.js.map