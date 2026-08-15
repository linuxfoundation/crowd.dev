"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const redis_1 = require("@crowd/redis");
const conf_1 = require("../../../conf");
const segmentRepository_1 = __importDefault(require("../../../database/repositories/segmentRepository"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const integrationService_1 = __importDefault(require("../../../services/integrationService"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
const errorURL = `${conf_1.API_CONFIG.frontendUrl}/integrations?twitter-error=true`;
const decodeBase64Url = (data) => {
    data = data.replaceAll('-', '+').replaceAll('_', '/');
    while (data.length % 4) {
        data += '=';
    }
    return atob(data);
};
exports.default = async (req, res) => {
    // Checking we have permision to edit the integration
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.integrationEdit);
    const cache = new redis_1.RedisCache('twitterPKCE', req.redis, req.log);
    const userId = req.currentUser.id;
    const decodedState = decodeBase64Url(req.query.state);
    const externalState = JSON.parse(decodedState);
    const { handle } = externalState;
    const existingValue = await cache.get(userId);
    if (!existingValue) {
        res.redirect(errorURL);
    }
    const stateObj = JSON.parse(existingValue);
    await cache.delete(userId);
    if (stateObj.handle !== handle) {
        res.redirect(errorURL);
    }
    const callbackUrl = stateObj.callbackUrl;
    const redirectUrl = stateObj.redirectUrl;
    const codeVerifier = stateObj.codeVerifier;
    const segmentIds = stateObj.segmentIds;
    const oauthVerifier = req.query.code;
    const hashtags = stateObj.hashtags;
    // attach segments to request
    const segmentRepository = new segmentRepository_1.default(req);
    req.currentSegments = await segmentRepository.findInIds(segmentIds);
    try {
        const response = await axios_1.default.post('https://api.twitter.com/2/oauth2/token', {}, {
            params: {
                client_id: conf_1.TWITTER_CONFIG.clientId,
                code: oauthVerifier,
                grant_type: 'authorization_code',
                redirect_uri: callbackUrl,
                code_verifier: codeVerifier,
            },
            auth: {
                username: conf_1.TWITTER_CONFIG.clientId,
                password: conf_1.TWITTER_CONFIG.clientSecret,
            },
        });
        // with the token let's get user info
        const userResponse = await axios_1.default.get('https://api.twitter.com/2/users/me', {
            headers: {
                Authorization: `Bearer ${response.data.access_token}`,
            },
        });
        const twitterUserId = userResponse.data.data.id;
        const integrationData = {
            profileId: twitterUserId,
            token: response.data.access_token,
            refreshToken: response.data.refresh_token,
            hashtags,
        };
        await new integrationService_1.default(req).twitterCallback(integrationData);
        res.redirect(redirectUrl);
    }
    catch (error) {
        res.redirect(errorURL);
    }
};
//# sourceMappingURL=twitterAuthenticateCallback.js.map