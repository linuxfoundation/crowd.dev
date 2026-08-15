"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getMemberWorkExperiences = getMemberWorkExperiences;
const zod_1 = require("zod");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const mapper_1 = require("@/utils/mapper");
const validation_1 = require("@/utils/validation");
const paramsSchema = zod_1.z.object({
    memberId: zod_1.z.uuid(),
});
async function getMemberWorkExperiences(req, res) {
    var _a;
    const { memberId } = (0, validation_1.validateOrThrow)(paramsSchema, req.params);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const member = await (0, data_access_layer_1.findMemberById)(qx, memberId, [data_access_layer_1.MemberField.ID]);
    if (!member) {
        throw new common_1.NotFoundError('Member not found');
    }
    const orgsMap = await (0, data_access_layer_1.fetchManyMemberOrgsWithOrgData)(qx, [memberId], { withDomains: true });
    const workExperiences = (0, mapper_1.groupMemberOrganizations)((_a = orgsMap.get(memberId)) !== null && _a !== void 0 ? _a : []).map(mapper_1.toMemberWorkExperience);
    (0, api_1.ok)(res, { memberId, workExperiences });
}
//# sourceMappingURL=getMemberWorkExperiences.js.map