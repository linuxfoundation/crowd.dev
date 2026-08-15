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
    // cloudflare headers to get the real ip & country
    const ipAddress = req.headers['cf-connecting-ip'];
    const country = req.headers['cf-ipcountry'];
    req.body = {
        ...req.body,
        ipAddress,
        country,
    };
    const payload = await new productAnalyticsService_1.default(req).createSession(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=productSessionCreate.js.map