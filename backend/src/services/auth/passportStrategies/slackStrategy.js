"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getSlackStrategy = getSlackStrategy;
const node_fetch_1 = __importDefault(require("node-fetch"));
const passport_slack_1 = __importDefault(require("passport-slack"));
const types_1 = require("@crowd/types");
const conf_1 = require("../../../conf");
function getSlackStrategy() {
    return new passport_slack_1.default.Strategy({
        clientID: conf_1.SLACK_CONFIG.clientId,
        clientSecret: conf_1.SLACK_CONFIG.clientSecret,
        callbackURL: `${conf_1.API_CONFIG.url}/slack/callback`,
        authorizationURL: 'https://slack.com/oauth/v2/authorize',
        tokenURL: 'https://slack.com/api/oauth.v2.access',
        skipUserProfile: true,
        passReqToCallback: true,
    }, (req, accessToken, refreshToken, profile, done) => {
        if (!done) {
            throw new TypeError('Missing req in verifyCallback; did you enable passReqToCallback in your strategy?');
        }
        (0, node_fetch_1.default)('https://slack.com/api/team.info', {
            headers: { Authorization: `Bearer ${accessToken}` },
        })
            .then((res) => res.json())
            .then((res) => {
            const existingUser = req.user || {};
            return done(null, {
                ...existingUser,
                [types_1.PlatformType.SLACK]: {
                    botToken: accessToken,
                    teamId: res.team.id,
                },
            });
        });
    });
}
//# sourceMappingURL=slackStrategy.js.map