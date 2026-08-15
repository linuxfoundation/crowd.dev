"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const common_1 = require("@crowd/common");
const redis_1 = require("@crowd/redis");
const conf_1 = require("@/conf");
const permissions_1 = __importDefault(require("../../../security/permissions"));
const track_1 = __importDefault(require("../../../segment/track"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
const getRedditToken = async (redis, logger) => {
    const cache = new redis_1.RedisCache('reddit-global-access-token', redis, logger);
    const token = await cache.get('reddit-token');
    if (token) {
        return token;
    }
    const result = await axios_1.default.post('https://www.reddit.com/api/v1/access_token', 'grant_type=client_credentials', {
        auth: {
            username: conf_1.REDDIT_CONFIG.clientId,
            password: conf_1.REDDIT_CONFIG.clientSecret,
        },
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
        },
    });
    // cache for 30 minutes
    await cache.set('reddit-token', result.data.access_token, 30 * 60);
    return result.data.access_token;
};
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHasAny([
        permissions_1.default.values.integrationCreate,
        permissions_1.default.values.integrationEdit,
    ]);
    if (req.query.subreddit) {
        let token;
        try {
            token = await getRedditToken(req.redis, req.log);
        }
        catch (e) {
            req.log.error(e);
            return req.responseHandler.error(req, res, new common_1.Error400(req.language));
        }
        try {
            const result = await axios_1.default.post(`https://oauth.reddit.com/api/search_reddit_names`, `query=${req.query.subreddit}&exact=true`, {
                headers: {
                    ContentType: 'application/x-www-form-urlencoded',
                    Authorization: `Bearer ${token}`,
                },
            });
            if (result.status === 200 &&
                result.data.names &&
                result.data.names.includes(req.query.subreddit)) {
                (0, track_1.default)('Reddit: subreddit input', {
                    subreddit: req.query.subreddit,
                    valid: true,
                }, { ...req });
                return req.responseHandler.success(req, res, true);
            }
            // for other status codes we return error
            (0, track_1.default)('Reddit: subreddit input', {
                subreddit: req.query.subreddit,
                valid: false,
            }, { ...req });
            return req.responseHandler.error(req, res, new common_1.Error400(req.language));
        }
        catch (e) {
            req.log.error('Error validating subreddit', e);
            (0, track_1.default)('Reddit: subreddit input', {
                subreddit: req.query.subreddit,
                valid: false,
            }, { ...req });
            req.log.error(e);
            return req.responseHandler.error(req, res, new common_1.Error400(req.language));
        }
    }
    (0, track_1.default)('Reddit: subreddit input', {
        subreddit: req.query.subreddit,
        valid: false,
    }, { ...req });
    req.log.error('Reddit: subreddit input is empty');
    return req.responseHandler.error(req, res, new common_1.Error400(req.language));
};
//# sourceMappingURL=redditValidator.js.map