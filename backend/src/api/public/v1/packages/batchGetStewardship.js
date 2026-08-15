"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.batchGetStewardship = batchGetStewardship;
const data_access_layer_1 = require("@crowd/data-access-layer");
const packagesDb_1 = require("@/db/packagesDb");
const api_1 = require("@/utils/api");
const validation_1 = require("@/utils/validation");
const purl_1 = require("./purl");
const bodySchema = (0, purl_1.purlsBodySchema)();
async function batchGetStewardship(req, res) {
    var _a;
    const { purls: rawPurls } = (0, validation_1.validateOrThrow)(bodySchema, req.body);
    // Normalize after parsing (not in the schema) so rawPurls keeps the client's
    // original form — used as the response key so clients can look up their input.
    const normalizedPurls = rawPurls.map(purl_1.normalizePurl);
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const rows = await (0, data_access_layer_1.getPackagesByStewardshipPurls)(qx, normalizedPurls);
    const byPurl = new Map(rows.map((r) => [r.purl, r]));
    const packages = {};
    for (let i = 0; i < rawPurls.length; i++) {
        const row = byPurl.get(normalizedPurls[i]);
        if (!row) {
            packages[rawPurls[i]] = null;
        }
        else {
            packages[rawPurls[i]] = {
                name: row.name,
                ecosystem: row.ecosystem,
                lifecycle: null,
                health: null,
                impact: row.criticalityScore != null ? Math.round(Number(row.criticalityScore) * 100) : null,
                openVulns: null,
                stewardship: ((_a = row.stewardshipStatus) !== null && _a !== void 0 ? _a : 'unassigned'),
                stewards: null,
                lastActivityAt: null,
                lastActivityDescription: null,
            };
        }
    }
    (0, api_1.ok)(res, { packages });
}
//# sourceMappingURL=batchGetStewardship.js.map