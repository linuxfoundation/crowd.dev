"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const conf_1 = require("../../../../conf");
async function getToken(connectionId, providerConfigKey, logger) {
    try {
        const url = `${conf_1.NANGO_CONFIG.url}/connection/${connectionId}`;
        const secretKey = conf_1.NANGO_CONFIG.secretKey;
        const headers = {
            'Content-Type': 'application/json',
            Authorization: `Basic ${Buffer.from(`${secretKey}:`).toString('base64')}`,
        };
        logger.debug({ secretKey, connectionId, providerConfigKey }, 'Fetching Nango token!');
        const params = {
            provider_config_key: providerConfigKey,
        };
        const response = await axios_1.default.get(url, { params, headers });
        return response.data.credentials.access_token;
    }
    catch (err) {
        logger.error({ err }, 'Error while getting token from Nango');
        throw err;
    }
}
exports.default = getToken;
//# sourceMappingURL=getToken.js.map