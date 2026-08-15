"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const authService_1 = __importDefault(require("../../services/auth/authService"));
exports.default = async (req, res) => {
    if (!req.body.acceptedTermsAndPrivacy) {
        return res.status(422).send({ error: 'Please accept terms of service and privacy policy' });
    }
    const payload = await authService_1.default.signup(req.body.email, req.body.password, req.body.invitationToken, common_1.DEFAULT_TENANT_ID, req.body.firstName, req.body.lastName, req.body.acceptedTermsAndPrivacy, req);
    return req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=authSignUp.js.map