"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const productAnalyticsService_1 = __importDefault(require("@/services/productAnalyticsService"));
exports.default = async (req, res) => {
    if (!req.currentUser || !req.currentUser.id) {
        throw new common_1.Error403(req.language);
    }
    await new productAnalyticsService_1.default(req).updateSession(req.params.id, req.body);
    const payload = true;
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=productSessionUpdate.js.map