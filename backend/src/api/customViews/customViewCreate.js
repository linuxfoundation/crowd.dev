"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const track_1 = __importDefault(require("../../segment/track"));
const customViewService_1 = __importDefault(require("../../services/customViewService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /customview
 * @summary Create a custom view
 * @tag CustomViews
 * @security Bearer
 * @description Create a custom view
 * @bodyContent {CustomViewInput} application/json
 * @response 200 - Ok
 * @responseContent {CustomView} 200.application/json
 * @responseExample {CustomViewCreate} 200.application/json.CustomView
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.customViewCreate);
    const payload = await new customViewService_1.default(req).create(req.body);
    (0, track_1.default)('Custom view Manually Created', { ...req.body }, { ...req });
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=customViewCreate.js.map