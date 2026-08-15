"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.passportStrategyMiddleware = passportStrategyMiddleware;
const passport_1 = __importDefault(require("passport"));
const logging_1 = require("@crowd/logging");
const conf_1 = require("../conf");
const githubStrategy_1 = require("../services/auth/passportStrategies/githubStrategy");
const googleStrategy_1 = require("../services/auth/passportStrategies/googleStrategy");
const slackStrategy_1 = require("../services/auth/passportStrategies/slackStrategy");
const log = (0, logging_1.getServiceLogger)();
async function passportStrategyMiddleware(req, res, next) {
    try {
        // if (TWITTER_CONFIG.clientId) {
        //   passport.use(getTwitterStrategy(req.redis, req.log))
        // }
        if (conf_1.SLACK_CONFIG.clientId) {
            passport_1.default.use((0, slackStrategy_1.getSlackStrategy)());
        }
        if (conf_1.GOOGLE_CONFIG.clientId) {
            passport_1.default.use((0, googleStrategy_1.getGoogleStrategy)());
        }
        if (conf_1.GITHUB_CONFIG.clientId) {
            passport_1.default.use((0, githubStrategy_1.getGithubStrategy)());
        }
    }
    catch (error) {
        log.error(error, 'Error getting some passport strategies!');
    }
    finally {
        next();
    }
}
//# sourceMappingURL=passportStrategyMiddleware.js.map