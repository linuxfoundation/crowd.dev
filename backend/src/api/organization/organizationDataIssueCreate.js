"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const types_1 = require("@crowd/types");
const dataIssueService_1 = __importDefault(require("@/services/dataIssueService"));
const permissions_1 = __importDefault(require("../../security/permissions"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.dataIssueCreate);
    const payload = await new dataIssueService_1.default(req).createDataIssue({ ...req.body, entity: types_1.DataIssueEntity.ORGANIZATION }, req.params.id);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=organizationDataIssueCreate.js.map