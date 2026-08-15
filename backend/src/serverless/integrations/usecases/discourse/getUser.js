"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDiscourseUserByUsername = void 0;
const axios_1 = __importDefault(require("axios"));
const types_1 = require("@crowd/types");
const getDiscourseUserByUsername = async (params, input, logger) => {
    var _a;
    logger.info({
        message: 'Fetching user by username from Discourse',
        params,
        input,
    });
    const config = {
        method: 'get',
        url: `${params.forumHostname}/u/${encodeURIComponent(input.username)}.json`,
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
            throw new types_1.RateLimitError(5 * 60, 'discourse/getuserbyusername');
        }
        if (((_a = err === null || err === void 0 ? void 0 : err.response) === null || _a === void 0 ? void 0 : _a.status) === 404) {
            return undefined;
        }
        logger.error({ err, params, input }, 'Error while fetching user by username from Discourse ');
        throw err;
    }
};
exports.getDiscourseUserByUsername = getDiscourseUserByUsername;
//# sourceMappingURL=getUser.js.map