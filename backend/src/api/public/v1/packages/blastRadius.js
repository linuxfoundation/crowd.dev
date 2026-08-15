"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.blastRadiusJobRequestSchema = exports.SUPPORTED_BLAST_RADIUS_ECOSYSTEMS = void 0;
exports.toBlastRadiusJobEntry = toBlastRadiusJobEntry;
const zod_1 = require("zod");
exports.SUPPORTED_BLAST_RADIUS_ECOSYSTEMS = [
    'npm',
    'go',
    'maven',
    'cargo',
    'nuget',
    'rubygems',
    'pypi',
];
// Always exactly one job per request — advisory-wide (package omitted) or narrowed
// to a single package. package accepts either a full purl or a bare package name,
// so it is NOT run through purlFieldSchema/normalizePurl like the other endpoints.
const ADVISORY_ID_PATTERN = /^(GHSA-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}|CVE-\d{4}-\d{4,})$/;
exports.blastRadiusJobRequestSchema = zod_1.z.object({
    advisoryId: zod_1.z
        .string()
        .trim()
        .regex(ADVISORY_ID_PATTERN, 'advisoryId must be a GHSA or CVE identifier'),
    ecosystem: zod_1.z.enum(exports.SUPPORTED_BLAST_RADIUS_ECOSYSTEMS, {
        error: `Ecosystem is not supported for blast-radius analysis — only ${exports.SUPPORTED_BLAST_RADIUS_ECOSYSTEMS.join(', ')} supported today`,
    }),
    package: zod_1.z.string().trim().min(1).nullish(),
    force: zod_1.z.boolean().default(false),
});
// Builds the 2a response body. The pipeline isn't implemented yet, so every freshly
// submitted job comes back pending — see analyzeBlastRadius in packages_worker.
function toBlastRadiusJobEntry(params) {
    return {
        analysisId: params.analysisId,
        advisoryId: params.advisoryId,
        package: params.package,
        ecosystem: params.ecosystem,
        status: 'pending',
    };
}
//# sourceMappingURL=blastRadius.js.map