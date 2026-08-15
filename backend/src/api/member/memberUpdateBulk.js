"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const memberService_1 = __importDefault(require("../../services/memberService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberEdit);
    const membersToUpdate = req.body;
    const memberService = new memberService_1.default(req);
    const promises = membersToUpdate.map((item) => memberService.update(item.id, item));
    const payload = await Promise.all(promises);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberUpdateBulk.js.map