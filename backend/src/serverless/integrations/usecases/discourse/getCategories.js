"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDiscourseCategories = void 0;
const axios_1 = __importDefault(require("axios"));
const types_1 = require("@crowd/types");
const getDiscourseCategories = async (params, logger) => {
    logger.info({
        message: 'Fetching categories from Discourse',
        forumHostName: params.forumHostname,
    });
    const config = {
        method: 'get',
        url: `${params.forumHostname}/categories.json`,
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
            throw new types_1.RateLimitError(5 * 60, 'discourse/getcategories');
        }
        logger.error({ err, params }, 'Error while getting Discourse categories');
        throw err;
    }
};
exports.getDiscourseCategories = getDiscourseCategories;
//# sourceMappingURL=getCategories.js.map