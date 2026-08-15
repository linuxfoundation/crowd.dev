"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const permissions_1 = __importDefault(require("../../security/permissions"));
const organizationService_1 = __importDefault(require("../../services/organizationService"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    var _a, _b;
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.organizationEdit);
    const identity = {
        ...req.query.identity,
        verified: ((_b = (_a = req.query) === null || _a === void 0 ? void 0 : _a.identity) === null || _b === void 0 ? void 0 : _b.verified) === 'true',
    };
    const payload = await new organizationService_1.default(req).canRevertMerge(req.params.organizationId, identity);
    await req.responseHandler.success(req, res, payload, 200);
};
//# sourceMappingURL=organizationCanRevertMerge.js.map