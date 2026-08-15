"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAffiliations = getAffiliations;
const zod_1 = require("zod");
const data_access_layer_1 = require("@crowd/data-access-layer");
const sequelizeQueryExecutor_1 = require("@/database/sequelizeQueryExecutor");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const MAX_HANDLES = 100;
const DEFAULT_PAGE_SIZE = 20;
const bodySchema = zod_1.z.object({
    githubHandles: zod_1.z
        .array(zod_1.z.string().trim().min(1).toLowerCase())
        .min(1)
        .max(MAX_HANDLES, `Maximum ${MAX_HANDLES} handles per request`),
});
const querySchema = zod_1.z.object({
    page: zod_1.z.coerce.number().int().min(1).default(1),
    pageSize: zod_1.z.coerce.number().int().min(1).max(MAX_HANDLES).default(DEFAULT_PAGE_SIZE),
});
async function getAffiliations(req, res) {
    var _a;
    const { githubHandles } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    const { page, pageSize } = (0, validation_1.validateOrThrow)(querySchema, req.query);
    const qx = (0, sequelizeQueryExecutor_1.optionsQx)(req);
    const offset = (page - 1) * pageSize;
    // Step 1: find all verified members across all handles
    const allMemberRows = await (0, data_access_layer_1.findMembersByGithubHandles)(qx, githubHandles);
    const foundHandles = new Set(allMemberRows.map((r) => r.githubHandle.toLowerCase()));
    const notFound = githubHandles.filter((h) => !foundHandles.has(h));
    const pageMemberRows = allMemberRows.slice(offset, offset + pageSize);
    if (pageMemberRows.length === 0) {
        (0, api_1.ok)(res, {
            total: githubHandles.length,
            totalFound: allMemberRows.length,
            page,
            pageSize,
            contributorsInPage: 0,
            contributors: [],
            notFound,
        });
        return;
    }
    const memberIds = pageMemberRows.map((r) => r.memberId);
    // Step 2: fetch verified emails for current page
    const emailRows = await (0, data_access_layer_1.findVerifiedEmailsByMemberIds)(qx, memberIds);
    const emailsByMember = new Map();
    for (const row of emailRows) {
        const list = (_a = emailsByMember.get(row.memberId)) !== null && _a !== void 0 ? _a : [];
        list.push(row.email);
        emailsByMember.set(row.memberId, list);
    }
    // Step 3: resolve affiliations for current page only
    const affiliationsByMember = await (0, data_access_layer_1.resolveAffiliationsByMemberIds)(qx, memberIds);
    // Step 4: build response
    const contributors = pageMemberRows.map((member) => {
        var _a, _b;
        return ({
            githubHandle: member.githubHandle,
            name: member.displayName,
            emails: (_a = emailsByMember.get(member.memberId)) !== null && _a !== void 0 ? _a : [],
            affiliations: (_b = affiliationsByMember.get(member.memberId)) !== null && _b !== void 0 ? _b : [],
        });
    });
    (0, api_1.ok)(res, {
        total: githubHandles.length,
        totalFound: allMemberRows.length,
        page,
        pageSize,
        contributorsInPage: contributors.length,
        contributors,
        notFound,
    });
}
//# sourceMappingURL=getAffiliations.js.map