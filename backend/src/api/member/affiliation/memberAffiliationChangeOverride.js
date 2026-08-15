"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const memberAffiliationsService_1 = __importDefault(require("@/services/member/memberAffiliationsService"));
const permissions_1 = __importDefault(require("../../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberEdit);
    const memberAffiliationsService = new memberAffiliationsService_1.default(req);
    const payload = await memberAffiliationsService.changeAffiliationOverride({
        ...req.body,
        memberId: req.params.memberId,
    });
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=memberAffiliationChangeOverride.js.map