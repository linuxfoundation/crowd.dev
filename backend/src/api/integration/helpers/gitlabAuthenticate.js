"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.generateRandomString = void 0;
const crypto_1 = __importDefault(require("crypto"));
const common_1 = require("@crowd/common");
const conf_1 = require("../../../conf");
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
/// end credits
exports.default = async (req, res) => {
    // Checking we have permision to edit the project
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.integrationEdit);
    const handle = (0, common_1.generateUUIDv4)();
    const callbackUrl = conf_1.GITLAB_CONFIG.callbackUrl;
    const gitlabState = {
        crowdToken: req.query.crowdToken,
        tenantId: req.params.tenantId,
        handle,
    };
    const scopes = ['api', 'read_api', 'read_user', 'read_repository', 'profile', 'email'];
    // Build the authorization URL
    const authUrl = createUrl('https://gitlab.com/oauth/authorize', {
        client_id: conf_1.GITLAB_CONFIG.clientId,
        response_type: 'code',
        state: encodeBase64Url(JSON.stringify(gitlabState)),
        redirect_uri: callbackUrl,
        scope: scopes.join(' '),
    });
    // Redirect user to the authorization URL
    res.redirect(authUrl.toString());
};
//# sourceMappingURL=gitlabAuthenticate.js.map