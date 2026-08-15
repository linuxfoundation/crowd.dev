"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const memberAttributeSettingsService_1 = __importDefault(require("../../services/memberAttributeSettingsService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /settings/members/attributes
 * @summary Attribute settings: create
 * @tag Members
 * @security Bearer
 * @description Create a members' attribute setting
 * @bodyContent {MemberAttributeSettingsCreateInput} application/json
 * @response 200 - Ok
 * @responseContent {MemberAttributeSettings} 200.application/json
 * @responseExample {MemberAttributeSettings} 200.application/json.MemberAttributeSettings
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberAttributesCreate);
    const payload = await new memberAttributeSettingsService_1.default(req).create(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberAttributeCreate.js.map