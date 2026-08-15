"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.generateRandomString = void 0;
const crypto_1 = __importDefault(require("crypto"));
const common_1 = require("@crowd/common");
const redis_1 = require("@crowd/redis");
const types_1 = require("@crowd/types");
const conf_1 = require("../../../conf");
const sequelizeRepository_1 = __importDefault(require("../../../database/repositories/sequelizeRepository"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
/// credits to lucia-auth library for these functions
const createUrl = (url, urlSearchParams) => {
    const newUrl = new URL(url);
    for (const [key, value] of Object.entries(urlSearchParams)) {
        // eslint-disable-next-line no-continue
        if (!value)
            continue;
        newUrl.searchParams.set(key, value);
    }
    return newUrl;
};
const getRandomValues = (bytes) => {
    const buffer = crypto_1.default.randomBytes(bytes);
    return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
};
const DEFAULT_ALPHABET = 'abcdefghijklmnopqrstuvwxyz1234567890';
const generateRandomString = (size, alphabet = DEFAULT_ALPHABET) => {
    var _a;
    // eslint-disable-next-line no-bitwise
    const mask = (2 << (Math.log(alphabet.length - 1) / Math.LN2)) - 1;
    // eslint-disable-next-line no-bitwise
    const step = -~((1.6 * mask * size) / alphabet.length);
    let bytes = getRandomValues(step);
    let id = '';
    let index = 0;
    while (id.length !== size) {
        // eslint-disable-next-line no-bitwise
        id += (_a = alphabet[bytes[index] & mask]) !== null && _a !== void 0 ? _a : '';
        index += 1;
        if (index > bytes.length) {
            bytes = getRandomValues(step);
            index = 0;
        }
    }
    return id;
};
exports.generateRandomString = generateRandomString;
const encodeBase64 = (data) => {
    if (typeof Buffer === 'function') {
        // node or bun
        const bufferData = typeof data === 'string' ? data : new Uint8Array(data);
        return Buffer.from(bufferData).toString('base64');
    }
    if (typeof data === 'string')
        return btoa(data);
    return btoa(String.fromCharCode(...new Uint8Array(data)));
};
const encodeBase64Url = (data) => encodeBase64(data).replaceAll('=', '').replaceAll('+', '-').replaceAll('/', '_');
const generatePKCECodeChallenge = (method, verifier) => {
    if (method === 'S256') {
        const hash = crypto_1.default.createHash('sha256');
        hash.update(verifier);
        const challengeBuffer = hash.digest();
        return encodeBase64Url(challengeBuffer);
    }
    throw new TypeError('Invalid PKCE code challenge method');
};
/// end credits
exports.default = async (req, res) => {
    // Checking we have permision to edit the project
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.integrationEdit);
    const cache = new redis_1.RedisCache('twitterPKCE', req.redis, req.log);
    // Generate code verifier and challenge for PKCE
    const codeVerifier = (0, exports.generateRandomString)(96, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_.~');
    const codeChallenge = generatePKCECodeChallenge('S256', codeVerifier);
    const handle = (0, common_1.generateUUIDv4)();
    const callbackUrl = conf_1.TWITTER_CONFIG.callbackUrl;
    const state = {
        handle,
        tenantId: req.params.tenantId,
        redirectUrl: req.query.redirectUrl,
        callbackUrl,
        hashtags: req.query.hashtags ? req.query.hashtags : '',
        crowdToken: req.query.crowdToken,
        platform: types_1.PlatformType.TWITTER,
        userId: req.currentUser.id,
        codeVerifier,
        segmentIds: sequelizeRepository_1.default.getSegmentIds(req),
    };
    const twitterState = {
        crowdToken: req.query.crowdToken,
        tenantId: req.params.tenantId,
        handle,
    };
    // Save state to redis
    await cache.set(req.currentUser.id, JSON.stringify(state), 300);
    const scopes = ['tweet.read', 'tweet.write', 'users.read', 'follows.read', 'offline.access'];
    // Build the authorization URL
    const authUrl = createUrl('https://twitter.com/i/oauth2/authorize', {
        client_id: conf_1.TWITTER_CONFIG.clientId,
        response_type: 'code',
        state: encodeBase64Url(JSON.stringify(twitterState)),
        redirect_uri: callbackUrl,
        code_challenge: codeChallenge,
        code_challenge_method: 'S256',
        scope: scopes.join(' '),
    });
    // Redirect user to the authorization URL
    res.redirect(authUrl.toString());
};
//# sourceMappingURL=twitterAuthenticate.js.map