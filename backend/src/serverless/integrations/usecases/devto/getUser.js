"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getUserByUsername = exports.getUserById = void 0;
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
/**
 * Performs a lookup of a Dev.to user by user id
 * @param userId
 * @returns {DevtoUser} or null if no user found
 */
const getUserById = async (userId) => {
    try {
        const result = await axios_1.default.get(`https://dev.to/api/users/${userId}`);
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
                    return (0, exports.getUserById)(userId);
                }
            }
        }
        else if (err.response.status === 404) {
            return null;
        }
        throw err;
    }
};
exports.getUserById = getUserById;
/**
 * Performs a lookup of a Dev.to user by username
 * @param username
 * @returns {DevtoUser} or null if no user found
 */
const getUserByUsername = async (username, apiKey) => {
    try {
        const result = await axios_1.default.get('https://dev.to/api/users/by_username', {
            params: {
                url: username,
            },
            headers: {
                Accept: 'application/vnd.forem.api-v1+json',
                'api-key': apiKey || '',
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
                    return (0, exports.getUserByUsername)(username);
                }
            }
        }
        else if (err.response.status === 404) {
            return null;
        }
        throw err;
    }
};
exports.getUserByUsername = getUserByUsername;
//# sourceMappingURL=getUser.js.map