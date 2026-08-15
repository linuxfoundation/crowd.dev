"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = __importDefault(require("axios"));
const conf_1 = require("../../conf");
exports.default = async (req, res) => {
    const response = await axios_1.default.get(conf_1.OPEN_STATUS_API_CONFIG.baseUrl);
    return req.responseHandler.success(req, res, response.data);
};
//# sourceMappingURL=systemStatus.js.map