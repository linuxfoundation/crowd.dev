"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const settingsService_1 = __importDefault(require("../../services/settingsService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.settingsEdit);
    const payload = await settingsService_1.default.save(req.body.settings, req);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=settingsSave.js.map