"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const segmentService_1 = __importDefault(require("../../services/segmentService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.segmentRead);
    const segmentService = new segmentService_1.default(req);
    const payload = await segmentService.findById(req.params.segmentId);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=segmentFind.js.map