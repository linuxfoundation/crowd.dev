"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.staticApiKeyMiddleware = staticApiKeyMiddleware;
const crypto_1 = __importDefault(require("crypto"));
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
function staticApiKeyMiddleware() {
    return async (req, _res, next) => {
        try {
            const authHeader = req.headers.authorization;
            if (!authHeader || !authHeader.startsWith('Bearer ')) {
                next(new common_1.UnauthorizedError('Missing or invalid Authorization header'));
                return;
            }
            const providedKey = authHeader.slice('Bearer '.length);
            const keyHash = crypto_1.default.createHash('sha256').update(providedKey).digest('hex');
            const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
            const apiKey = await (0, data_access_layer_1.findApiKeyByHash)(qx, keyHash);
            if (!apiKey) {
                next(new common_1.UnauthorizedError('Invalid API key'));
                return;
            }
            if (apiKey.revokedAt) {
                next(new common_1.UnauthorizedError('API key has been revoked'));
                return;
            }
            if (apiKey.expiresAt && apiKey.expiresAt < new Date()) {
                next(new common_1.UnauthorizedError('API key has expired'));
                return;
            }
            // fire and forget — don't block the request
            (0, data_access_layer_1.touchApiKeyLastUsed)(qx, apiKey.id).catch(() => { });
            req.actor = { id: apiKey.name, type: 'service', scopes: apiKey.scopes };
            next();
        }
        catch (err) {
            next(err);
        }
    };
}
//# sourceMappingURL=staticApiKeyMiddleware.js.map