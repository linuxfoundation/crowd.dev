"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toAkritesExternalContactDetail = toAkritesExternalContactDetail;
const data_access_layer_1 = require("@crowd/data-access-layer");
const mappers_1 = require("./mappers");
function toAkritesExternalContactDetail(row) {
    var _a, _b, _c, _d, _e, _f, _g;
    const contacts = ((_a = row.securityContacts) !== null && _a !== void 0 ? _a : []).map((c) => ({
        channel: c.channel,
        value: c.value,
        role: c.role,
        confidenceBand: c.confidence,
        confidenceScore: Number(c.score),
    }));
    // Aggregate band derives from the highest-scoring contact (same rule as the internal
    // packageConfidence), returned verbatim. NONE when there are no contacts.
    const overallConfidenceBand = contacts.length > 0
        ? (0, data_access_layer_1.securityContactConfidenceBand)(Math.max(...contacts.map((c) => c.confidenceScore)))
        : 'NONE';
    return {
        purl: row.purl,
        name: row.name,
        ecosystem: row.ecosystem,
        overallConfidenceBand,
        contacts,
        securityPolicyUrl: (_b = row.securityPolicyUrl) !== null && _b !== void 0 ? _b : null,
        vulnerabilityReportingUrl: (_c = row.vulnerabilityReportingUrl) !== null && _c !== void 0 ? _c : null,
        bugBountyUrl: (_d = row.bugBountyUrl) !== null && _d !== void 0 ? _d : null,
        pvrEnabled: (_e = row.pvrEnabled) !== null && _e !== void 0 ? _e : null,
        declaredRepositoryUrl: (_f = row.declaredRepositoryUrl) !== null && _f !== void 0 ? _f : null,
        resolvedRepositoryUrl: (_g = row.resolvedRepositoryUrl) !== null && _g !== void 0 ? _g : null,
        repoMappingConfidence: (0, mappers_1.toNullableNumber)(row.repoMappingConfidence),
    };
}
//# sourceMappingURL=akritesExternalContactDetail.js.map