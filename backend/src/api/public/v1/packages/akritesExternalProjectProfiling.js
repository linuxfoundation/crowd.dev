"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toAkritesExternalProjectProfiling = toAkritesExternalProjectProfiling;
// Timestamptz columns arrive as the raw Postgres string (OID 1184 parser returns it
// verbatim), so normalize to canonical ISO 8601. Returns null for null/unparseable.
function toIsoOrNull(value) {
    if (value == null)
        return null;
    const ms = Date.parse(value);
    return Number.isNaN(ms) ? null : new Date(ms).toISOString();
}
function toAkritesExternalProjectProfiling(row) {
    var _a, _b, _c, _d;
    return {
        purl: row.purl,
        declared: row.declared,
        methods: (_a = row.methods) !== null && _a !== void 0 ? _a : [],
        guidelines: (_b = row.guidelines) !== null && _b !== void 0 ? _b : null,
        sources: (_c = row.sources) !== null && _c !== void 0 ? _c : [],
        bugBountyUrl: (_d = row.bugBountyUrl) !== null && _d !== void 0 ? _d : null,
        assembledAt: toIsoOrNull(row.assembledAt),
    };
}
//# sourceMappingURL=akritesExternalProjectProfiling.js.map