"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.OrganizationQueryCache = void 0;
const crypto_1 = require("crypto");
const logging_1 = require("@crowd/logging");
const redis_1 = require("@crowd/redis");
const log = (0, logging_1.getServiceLogger)();
class OrganizationQueryCache {
    constructor(redis) {
        this.cache = new redis_1.RedisCache('organizations-advanced', redis, log);
        this.countCache = new redis_1.RedisCache('organizations-count', redis, log);
    }
    static buildCacheKey(params) {
        var _a;
        const cleanParams = Object.fromEntries(Object.entries({
            countOnly: params.countOnly,
            fields: (_a = params.fields) === null || _a === void 0 ? void 0 : _a.sort(),
            filter: params.filter,
            include: params.include,
            limit: params.limit,
            offset: params.offset,
            orderBy: params.orderBy,
            search: params.search,
            segmentId: params.segmentId,
        }).filter(([, value]) => value !== null && value !== undefined));
        const hash = (0, crypto_1.createHash)('md5').update(JSON.stringify(cleanParams)).digest('hex');
        return `organizations_advanced_${hash}`;
    }
    async get(cacheKey) {
        try {
            const cachedResult = await this.cache.get(cacheKey);
            if (cachedResult) {
                return JSON.parse(cachedResult);
            }
            return null;
        }
        catch (error) {
            log.warn('Error retrieving from cache', { error });
            return null;
        }
    }
    async set(cacheKey, result, ttlSeconds) {
        try {
            await this.cache.set(cacheKey, JSON.stringify(result), ttlSeconds);
        }
        catch (error) {
            log.warn('Error saving to cache', { error });
        }
    }
    async getCount(cacheKey) {
        try {
            const cachedCount = await this.countCache.get(cacheKey);
            return cachedCount ? parseInt(cachedCount, 10) : null;
        }
        catch (error) {
            log.warn('Error retrieving count from cache', { error });
            return null;
        }
    }
    async setCount(cacheKey, count, ttlSeconds) {
        try {
            await this.countCache.set(cacheKey, count.toString(), ttlSeconds);
        }
        catch (error) {
            log.warn('Error saving count to cache', { error });
        }
    }
}
exports.OrganizationQueryCache = OrganizationQueryCache;
//# sourceMappingURL=organizationsQueryCache.js.map