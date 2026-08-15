"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.oauth2Middleware = oauth2Middleware;
const express_oauth2_jwt_bearer_1 = require("express-oauth2-jwt-bearer");
const common_1 = require("@crowd/common");
function resolveIssuer(req) {
    var _a;
    const token = (_a = req.headers.authorization) === null || _a === void 0 ? void 0 : _a.split(' ')[1];
    if (!token)
        return undefined;
    try {
        const { iss } = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString());
        return typeof iss === 'string' ? iss : undefined;
    }
    catch (_b) {
        return undefined;
    }
}
function resolveActor(req, _res, next) {
    var _a, _b, _c;
    const payload = ((_b = (_a = req.auth) === null || _a === void 0 ? void 0 : _a.payload) !== null && _b !== void 0 ? _b : {});
    const rawId = (_c = payload.sub) !== null && _c !== void 0 ? _c : payload.azp;
    if (!rawId) {
        next(new common_1.UnauthorizedError('Token missing caller identity'));
        return;
    }
    const id = rawId.replace(/@clients$/, '');
    const scopes = typeof payload.scope === 'string' ? payload.scope.split(' ').filter(Boolean) : [];
    req.actor = { id, type: 'service', scopes };
    next();
}
function oauth2Middleware(config) {
    const issuers = config.issuerBaseURLs
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
    if (issuers.length === 0) {
        throw new Error('No auth0 issuers configured');
    }
    const handlersByIssuer = new Map(issuers.map((issuerBaseURL) => [
        issuerBaseURL.replace(/\/$/, ''),
        (0, express_oauth2_jwt_bearer_1.auth)({ issuerBaseURL, audience: config.audience }),
    ]));
    const verifyJwt = (req, res, next) => {
        const iss = resolveIssuer(req);
        if (!iss) {
            next(new common_1.UnauthorizedError('Missing or malformed bearer token'));
            return;
        }
        const handler = handlersByIssuer.get(iss.replace(/\/$/, ''));
        if (!handler) {
            next(new common_1.UnauthorizedError('Unknown token issuer'));
            return;
        }
        handler(req, res, next);
    };
    return [verifyJwt, resolveActor];
}
//# sourceMappingURL=oauth2Middleware.js.map