"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const userRepository_1 = __importDefault(require("../../database/repositories/userRepository"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.userAutocomplete);
    const payload = await userRepository_1.default.findAllAutocomplete(req.query.query, req.query.limit, req);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=userAutocomplete.js.map