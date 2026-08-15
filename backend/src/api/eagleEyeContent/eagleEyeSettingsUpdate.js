"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const eagleEyeSettingsService_1 = __importDefault(require("../../services/eagleEyeSettingsService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.eagleEyeActionCreate);
    const payload = await new eagleEyeSettingsService_1.default(req).update(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=eagleEyeSettingsUpdate.js.map