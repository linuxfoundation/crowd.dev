"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ingestAkritesExternalContactDetail = ingestAkritesExternalContactDetail;
const crypto_1 = require("crypto");
const common_1 = require("@crowd/common");
const data_access_layer_1 = require("@crowd/data-access-layer");
const temporal_1 = require("@crowd/temporal");
const types_1 = require("@crowd/types");
const packagesDb_1 = require("@/db/packagesDb");
const packagesTemporal_1 = require("@/db/packagesTemporal");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const akritesExternalContactDetail_1 = require("./akritesExternalContactDetail");
const purl_1 = require("./purl");
// Deterministic, purl-derived workflowId: concurrent callers hitting the same purl attach
// to the same running workflow (USE_EXISTING) instead of each starting their own ingest —
// same pattern as integrationService.ts's github-nango-sync workflow start.
function ingestWorkflowId(purl) {
    return `${types_1.TemporalWorkflowId.SECURITY_CONTACTS_ONDEMAND}:${(0, crypto_1.createHash)('sha256').update(purl).digest('hex')}`;
}
// Sync, single-purl on-demand ingest. Blocks on the worker's single-repo activity
// (security-contacts/workflows.ts's singleActs: 45s startToCloseTimeout x 2 attempts,
// worst case ~95s, plus unbounded time waiting for a free worker slot) — no batch
// variant, since fanning this out over many purls would multiply concurrent Temporal
// workflow starts.
//
// The underlying workflow itself has no "already processed" gate — it reprocesses and
// replaces a repo's contacts on every invocation — so this handler is the one place
// that must avoid re-triggering it for a purl that's already been ingested. Gate on
// repos.contacts_last_refreshed: null means never processed (trigger the workflow),
// non-null means a pass already ran (return what's there, even if it found no contacts).
async function ingestAkritesExternalContactDetail(req, res) {
    const { purl } = (0, validation_1.validateOrThrow)(purl_1.purlBodySchema, req.body);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const [existing] = await (0, data_access_layer_1.getContactDetailsByPurls)(qx, [purl]);
    if (existing === null || existing === void 0 ? void 0 : existing.contactsLastRefreshed) {
        (0, api_1.ok)(res, (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(existing));
        return;
    }
    if (!(existing === null || existing === void 0 ? void 0 : existing.resolvedRepositoryUrl)) {
        throw new common_1.NotFoundError('Purl has no linked repository to ingest security contacts from');
    }
    const packagesTemporal = await (0, packagesTemporal_1.getPackagesTemporalClient)();
    const result = await packagesTemporal.workflow.execute('ingestSecurityContactsForPurlWorkflow', {
        taskQueue: 'security-contacts-worker',
        workflowId: ingestWorkflowId(purl),
        workflowIdConflictPolicy: temporal_1.WorkflowIdConflictPolicy.USE_EXISTING,
        workflowIdReusePolicy: temporal_1.WorkflowIdReusePolicy.ALLOW_DUPLICATE,
        args: [purl],
    });
    if (!result.found) {
        throw new common_1.NotFoundError('Purl has no linked repository to ingest security contacts from');
    }
    const [row] = await (0, data_access_layer_1.getContactDetailsByPurls)(qx, [purl]);
    if (!row) {
        throw new common_1.NotFoundError('Contact detail not found after ingest');
    }
    (0, api_1.ok)(res, (0, akritesExternalContactDetail_1.toAkritesExternalContactDetail)(row));
}
//# sourceMappingURL=ingestAkritesExternalContactDetail.js.map