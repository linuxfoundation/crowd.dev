"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getGithubStrategy = getGithubStrategy;
const lodash_1 = require("lodash");
const passport_github2_1 = __importDefault(require("passport-github2"));
const logging_1 = require("@crowd/logging");
const types_1 = require("@crowd/types");
const conf_1 = require("../../../conf");
const databaseConnection_1 = require("../../../database/databaseConnection");
const splitName_1 = require("../../../utils/splitName");
const authService_1 = __importDefault(require("../authService"));
const log = (0, logging_1.getServiceChildLogger)('AuthSocial');
function getGithubStrategy() {
    return new passport_github2_1.default({
        clientID: conf_1.GITHUB_CONFIG.clientId,
        clientSecret: conf_1.GITHUB_CONFIG.clientSecret,
        callbackURL: conf_1.GITHUB_CONFIG.callbackUrl,
        scope: ['user:email'], // Request email scope
    }, (accessToken, refreshToken, profile, done) => {
        (0, databaseConnection_1.databaseInit)()
            .then((database) => {
            const email = (0, lodash_1.get)(profile, 'emails[0].value');
            // GitHub user's profile doesn't include 'verified' field
            // However, GitHub accounts require email verification for activation
            const emailVerified = !!email;
            const displayName = (0, lodash_1.get)(profile, 'displayName');
            const { firstName, lastName } = (0, splitName_1.splitFullName)(displayName);
            return authService_1.default.signinFromSocial(types_1.AuthProvider.GITHUB, profile.id, email, emailVerified, firstName, lastName, displayName, { database });
        })
            .then((jwtToken) => {
            done(null, jwtToken);
        })
            .catch((error) => {
            log.error(error, 'Error while handling github auth!');
            done(error, null);
        });
    });
}
//# sourceMappingURL=githubStrategy.js.map