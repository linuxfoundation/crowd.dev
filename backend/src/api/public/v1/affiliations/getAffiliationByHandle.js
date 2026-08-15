"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAffiliationByHandle = getAffiliationByHandle;
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
async function getAffiliationByHandle(req, res) {
    var _a;
    const handle = req.params.githubHandle.toLowerCase();
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const members = await (0, data_access_layer_1.findMembersByGithubHandles)(qx, [handle]);
    if (members.length === 0) {
        throw new common_1.NotFoundError(`No LFX profile found for GitHub login '${req.params.githubHandle}'.`);
    }
    const member = members[0];
    const memberIds = [member.memberId];
    const [emailRows, affiliationsByMember] = await Promise.all([
        (0, data_access_layer_1.findVerifiedEmailsByMemberIds)(qx, memberIds),
        (0, data_access_layer_1.resolveAffiliationsByMemberIds)(qx, memberIds),
    ]);
    (0, api_1.ok)(res, {
        githubHandle: member.githubHandle,
        name: member.displayName,
        emails: emailRows.map((r) => r.email),
        affiliations: (_a = affiliationsByMember.get(member.memberId)) !== null && _a !== void 0 ? _a : [],
    });
}
//# sourceMappingURL=getAffiliationByHandle.js.map