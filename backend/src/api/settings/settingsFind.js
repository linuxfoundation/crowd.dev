"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const settingsService_1 = __importDefault(require("../../services/settingsService"));
exports.default = async (req, res) => {
    const payload = await settingsService_1.default.findOrCreateDefault(req);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=settingsFind.js.map