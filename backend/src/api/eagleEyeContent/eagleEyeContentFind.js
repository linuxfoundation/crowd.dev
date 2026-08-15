"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const eagleEyeContentService_1 = __importDefault(require("../../services/eagleEyeContentService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.eagleEyeContentRead);
    const payload = await new eagleEyeContentService_1.default(req).findById(req.params.id);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=eagleEyeContentFind.js.map