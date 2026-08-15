"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.retryBackoff = retryBackoff;
const common_1 = require("@crowd/common");
async function retryBackoff(fn, maxRetries = 3) {
    let retries = 0;
    while (retries < maxRetries) {
        try {
            return await fn();
        }
        catch (error) {
            retries++;
            // Exponential backoff with base of 2 seconds
            // 1st retry: 2s, 2nd: 4s, 3rd: 8s, etc
            const backoffMs = 2000 * 2 ** (retries - 1);
            await (0, common_1.timeout)(backoffMs);
        }
    }
    throw new Error('Max retries reached');
}
//# sourceMappingURL=backoff.js.map