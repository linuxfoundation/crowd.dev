"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const authService_1 = __importDefault(require("../../services/auth/authService"));
exports.default = async (req, res) => {
    const payload = await authService_1.default.changePassword(req.body.oldPassword, req.body.newPassword, req);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=authPasswordChange.js.map