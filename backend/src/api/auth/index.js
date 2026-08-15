"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const errorMiddleware_1 = require("../../middlewares/errorMiddleware");
const apiRateLimiter_1 = require("../apiRateLimiter");
exports.default = (app) => {
    const signInRateLimiter = (0, apiRateLimiter_1.createRateLimiter)({
        max: 100,
        windowMs: 15 * 60 * 1000,
    });
    app.post(`/auth/sign-in`, signInRateLimiter, (0, errorMiddleware_1.safeWrap)(require('./authSignIn').default));
    const signUpRateLimiter = (0, apiRateLimiter_1.createRateLimiter)({
        max: 20,
        windowMs: 60 * 60 * 1000,
    });
    app.post(`/auth/sign-up`, signUpRateLimiter, (0, errorMiddleware_1.safeWrap)(require('./authSignUp').default));
    app.put(`/auth/profile`, (0, errorMiddleware_1.safeWrap)(require('./authUpdateProfile').default));
    app.put(`/auth/change-password`, (0, errorMiddleware_1.safeWrap)(require('./authPasswordChange').default));
    app.get(`/auth/me`, (0, errorMiddleware_1.safeWrap)(require('./authMe').default));
    app.post(`/auth/sso/callback`, (0, errorMiddleware_1.safeWrap)(require('./ssoCallback').default));
};
//# sourceMappingURL=index.js.map