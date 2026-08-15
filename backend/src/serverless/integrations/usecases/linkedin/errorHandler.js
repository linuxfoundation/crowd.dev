"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.handleLinkedinError = void 0;
const moment_1 = __importDefault(require("moment"));
const rateLimitError_1 = require("../../../../types/integration/rateLimitError");
const handleLinkedinError = (err, config, input, logger) => {
    const queryParams = [];
    if (config.params) {
        for (const [key, value] of Object.entries(config.params)) {
            queryParams.push(`${key}=${encodeURIComponent(value)}`);
        }
    }
    const url = `${config.url}?${queryParams.join('&')}`;
    // https://learn.microsoft.com/en-us/linkedin/shared/api-guide/concepts/rate-limits
    if (err && err.response && err.response.status === 429) {
        logger.warn('LinkedIn API rate limit exceeded');
        // we don't get information about when it resets because it resets every day at midnight (UTC)
        const now = (0, moment_1.default)().utcOffset(0);
        const nextMidnight = (0, moment_1.default)().utcOffset(0).add(1, 'day').startOf('day');
        const rateLimitResetSeconds = nextMidnight.diff(now, 'seconds');
        return new rateLimitError_1.RateLimitError(rateLimitResetSeconds, url, err);
    }
    logger.error(err, { input }, `Error while calling LinkedIn API URL: ${url}`);
    return err;
};
exports.handleLinkedinError = handleLinkedinError;
//# sourceMappingURL=errorHandler.js.map