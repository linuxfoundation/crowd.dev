"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const passport_1 = __importDefault(require("passport"));
const common_1 = require("@crowd/common");
const logging_1 = require("@crowd/logging");
const conf_1 = require("../../conf");
const authService_1 = __importDefault(require("../../services/auth/authService"));
const log = (0, logging_1.getServiceChildLogger)('AuthSocial');
exports.default = (app, routes) => {
    app.use(passport_1.default.initialize());
    passport_1.default.serializeUser((user, done) => {
        done(null, user);
    });
    passport_1.default.deserializeUser((user, done) => {
        done(null, user);
    });
    routes.post('/auth/social/onboard', async (req, res) => {
        const payload = await authService_1.default.handleOnboard(req.currentUser, { invitationToken: req.body.invitationToken, tenantId: common_1.DEFAULT_TENANT_ID }, req);
        await req.responseHandler.success(req, res, payload);
    });
    if (conf_1.GOOGLE_CONFIG.clientId) {
        routes.get('/auth/social/google', passport_1.default.authenticate('google', {
            scope: ['email', 'profile'],
            session: false,
        }), () => {
            // The request will be redirected for authentication, so this
            // function will not be called.
        });
        routes.get('/auth/social/google/callback', (req, res) => {
            passport_1.default.authenticate('google', (err, jwtToken) => {
                handleCallback(res, err, jwtToken);
            })(req, res);
        });
    }
    if (conf_1.GITHUB_CONFIG.clientId) {
        routes.get('/auth/social/github', passport_1.default.authenticate('github', {
            scope: ['user:email', 'read:user'],
            session: false,
        }), () => {
            // The request will be redirected for authentication, so this
            // function will not be called.
        });
        routes.get('/auth/social/github/callback', (req, res) => {
            passport_1.default.authenticate('github', (err, jwtToken) => {
                handleCallback(res, err, jwtToken);
            })(req, res);
        });
    }
};
function handleCallback(res, err, jwtToken) {
    if (err) {
        log.error(err, 'Error handling social callback!');
        let errorCode = 'generic';
        if (['auth-invalid-provider', 'auth-no-email'].includes(err.message)) {
            errorCode = err.message;
        }
        res.redirect(`${conf_1.API_CONFIG.frontendUrl}/auth/signin?socialErrorCode=${errorCode}`);
        return;
    }
    res.redirect(`${conf_1.API_CONFIG.frontendUrl}/?social=true&authToken=${jwtToken}`);
}
//# sourceMappingURL=authSocial.js.map