"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const authProfileEditor_1 = __importDefault(require("../../services/auth/authProfileEditor"));
exports.default = async (req, res) => {
    if (!req.currentUser || !req.currentUser.id) {
        throw new common_1.Error403(req.language);
    }
    const editor = new authProfileEditor_1.default(req);
    await editor.execute(req.body);
    const payload = true;
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=authUpdateProfile.js.map