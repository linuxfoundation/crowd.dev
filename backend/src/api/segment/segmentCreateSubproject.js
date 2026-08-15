"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const segmentService_1 = __importDefault(require("../../services/segmentService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.segmentCreate);
    const payload = await new segmentService_1.default(req).createSubproject(req.body);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=segmentCreateSubproject.js.map