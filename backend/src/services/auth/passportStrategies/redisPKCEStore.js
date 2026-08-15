"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
/* eslint-disable class-methods-use-this */
/* eslint-disable consistent-return */
/**
 * This class is a re-implementation of
 * https://github.com/jaredhanson/passport-oauth2/blob/master/lib/state/pkcesession.js
 * PKCE (Proof Key for Code Exchange) is a mechanism that makes OAuth2.0 Authorization Code grant
 * more secure to man in the middle attacks. It prevents the OAuth2 from happening when the authorization code
 * is leaked, and a third party tries to get access token using this leaked authorization code.
 * Original implementation was requiring sessions. But since we're making the authorization
 * on the backend, we didn't want sessions to keep the api stateless.
 * PKCE state is kept on a redis instance.
 *
 */
class RedisPKCEStore {
    constructor(redisCache) {
        this.cache = redisCache;
        this.verifier = '';
        this.state = '';
    }
    store(req, verifier, state, meta, callback) {
        if (!req.redis) {
            return callback(new Error('OAuth 2.0 authentication with PKCE requires redis support.'));
        }
        if (!state.userId) {
            return callback(new Error('userId in state is required for PKCE check.'));
        }
        let sstate = {
            handle: (0, common_1.generateUUIDv4)(),
            code_verifier: verifier,
        };
        if (state) {
            sstate = { ...sstate, ...state };
        }
        const stateSearchParams = new URLSearchParams({
            handle: sstate.handle,
            userId: state.userId,
            crowdToken: state.crowdToken,
            tenantId: state.tenantId,
            hashtags: state.hashtags,
            redirectUrl: state.redirectUrl,
        }).toString();
        this.cache.set(req.currentUser.id, JSON.stringify(sstate), 300).then(() => {
            callback(null, stateSearchParams);
        });
    }
    verify(req, providedState, callback) {
        if (!req.redis) {
            return callback(new Error('OAuth 2.0 authentication with PKCE requires redis support.'));
        }
        const params = new URLSearchParams(providedState);
        const handle = params.get('handle');
        const userId = params.get('userId');
        this.cache.get(userId).then((existingValue) => {
            if (!existingValue) {
                return callback(null, false, { message: 'Unable to verify authorization request state.' });
            }
            const stateObj = JSON.parse(existingValue);
            this.cache.delete(userId).then(() => {
                if (stateObj.handle !== handle) {
                    return callback(null, false, { message: 'Invalid authorization request state.' });
                }
                return callback(null, stateObj.code_verifier, stateObj);
            });
        });
    }
}
exports.default = RedisPKCEStore;
//# sourceMappingURL=redisPKCEStore.js.map