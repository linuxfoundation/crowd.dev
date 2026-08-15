"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const common_1 = require("@crowd/common");
const types_1 = require("@crowd/types");
const permissions_1 = __importDefault(require("../../security/permissions"));
const identifyTenant_1 = __importDefault(require("../../segment/identifyTenant"));
const track_1 = __importDefault(require("../../segment/track"));
const permissionChecker_1 = __importDefault(require("../../services/user/permissionChecker"));
/**
 * POST /member/export
 * @summary Export members as CSV
 * @tag Members
 * @security Bearer
 * @description Export members. It accepts filters, sorting options and pagination.
 * @bodyContent {MemberQuery} application/json
 * @response 200 - Ok
 * @response 401 - Unauthorized
 * @response 404 - Not found
 * @response 429 - Too many requests
 */
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.memberRead);
    await req.temporal.workflow.start('exportMembersToCSV', {
        taskQueue: 'exports',
        workflowId: `${types_1.TemporalWorkflowId.MEMBERS_CSV_EXPORTS}/${req.currentTenant.id}/${(0, common_1.generateUUIDv4)()}`,
        retry: {
            maximumAttempts: 1,
        },
        args: [
            {
                tenantId: req.currentTenant.id,
                segmentIds: req.body.segments,
                criteria: req.body,
                sendTo: [req.currentUser.email],
            },
        ],
        searchAttributes: {
            TenantId: [req.currentTenant.id],
        },
    });
    (0, identifyTenant_1.default)(req);
    (0, track_1.default)('Member CSV Export', {}, { ...req.body }, req.currentUser.id);
    await req.responseHandler.success(req, res, {});
};
//# sourceMappingURL=memberExport.js.map