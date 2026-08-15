"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../../security/permissions"));
const integrationService_1 = __importDefault(require("../../../services/integrationService"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    // Checking we have permision to edit the integration
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.integrationEdit);
    const { redirectUrl } = JSON.parse(Buffer.from(req.query.state, 'base64').toString());
    const integrationData = {
        token: req.account.slack.botToken,
        integrationIdentifier: req.account.slack.teamId,
    };
    await new integrationService_1.default(req).slackCallback(integrationData);
    res.redirect(redirectUrl);
};
//# sourceMappingURL=slackAuthenticateCallback.js.map