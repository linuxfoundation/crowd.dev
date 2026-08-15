"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const customViewService_1 = __importDefault(require("../../services/customViewService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * PUT /customview/{id}
 * @summary Update an custom view
 * @tag CustomViews
 * @security Bearer
 * @description Update an custom view given an ID.
 * @pathParam {string} id - The ID of the custom view
 * @bodyContent {CustomViewUpsertInput} application/json
 * @response 200 - Ok
 * @responseContent {CustomView} 200.application/json
 * @responseExample {CustomViewFind} 200.application/json.CustomView
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.customViewEdit);
    const payload = await new customViewService_1.default(req).update(req.params.id, req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=customViewUpdate.js.map