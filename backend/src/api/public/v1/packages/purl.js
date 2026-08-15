"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_BATCH_PAGE_SIZE = exports.MAX_PURLS_PER_BATCH = exports.purlFilterSchema = exports.purlBodySchema = exports.purlQuerySchema = exports.purlFieldSchema = void 0;
exports.normalizePurl = normalizePurl;
exports.purlsBodySchema = purlsBodySchema;
exports.paginatedPurlsBodySchema = paginatedPurlsBodySchema;
exports.paginatePurls = paginatePurls;
/**
 * Normalize a PURL for lookup against the packages table.
 *
 * The DB stores versionless PURLs with npm scope @ encoded as %40.
 * Clients may send purls with versions (pkg:npm/lodash@4.17.21) and qualifiers.
 *
 * Transform order:
 *   1. strip ?qualifiers and #subpath — not stored in DB
 *   2. strip @version suffix — DB stores versionless PURLs
 *   3. encode @ in namespace/scope (e.g. npm @babel → %40babel)
 *
 * The version regex matches @ followed by non-/ non-@ chars at end of string.
 * This is always the version separator, not an npm scope (pkg:npm/@babel/core
 * has @babel followed by /core, so it never matches the end-of-string pattern).
 */
const zod_1 = require("zod");
function stripQualifiers(purl) {
    const q = purl.indexOf('?');
    const h = purl.indexOf('#');
    if (q === -1 && h === -1)
        return purl;
    if (q === -1)
        return purl.slice(0, h);
    if (h === -1)
        return purl.slice(0, q);
    return purl.slice(0, Math.min(q, h));
}
function normalizePurl(purl) {
    const withoutQualifiers = stripQualifiers(purl);
    const withoutVersion = withoutQualifiers.replace(/@[^/@]+$/, '');
    return withoutVersion.replace(/@/g, '%40');
}
exports.purlFieldSchema = zod_1.z
    .string()
    .trim()
    .min(1)
    .refine((v) => v.startsWith('pkg:'), { message: 'purl must start with pkg:' })
    .transform(normalizePurl);
exports.purlQuerySchema = zod_1.z.object({ purl: exports.purlFieldSchema });
// Single-purl body (as opposed to purlsBodySchema's array) — normalizes like
// purlFieldSchema/purlQuerySchema, for endpoints that take exactly one purl in the body.
exports.purlBodySchema = zod_1.z.object({ purl: exports.purlFieldSchema });
// Loose schema for search filters: normalizes without requiring the pkg: prefix,
// so partial inputs (e.g. "@babel/core" or "lodash") are accepted.
exports.purlFilterSchema = zod_1.z.string().trim().transform(normalizePurl).optional();
exports.MAX_PURLS_PER_BATCH = 100;
// Unlike purlFieldSchema, this does NOT normalize — batch endpoints normalize
// after parsing so they can echo back the client's original (un-normalized) purl.
const purlArrayItemSchema = zod_1.z
    .string()
    .trim()
    .min(1)
    .refine((v) => v.startsWith('pkg:'), { message: 'each purl must start with pkg:' });
function purlsBodySchema(max = exports.MAX_PURLS_PER_BATCH) {
    return zod_1.z.object({
        purls: zod_1.z.array(purlArrayItemSchema).min(1).max(max, `Maximum ${max} purls per request`),
    });
}
exports.DEFAULT_BATCH_PAGE_SIZE = 20;
// Batch endpoints resolve the exact purls the client sends, then return one page of
// results (request order). page/pageSize mirror the GET /packages list contract;
// pageSize caps at the batch max so a single page can still cover a full request.
function paginatedPurlsBodySchema(max = exports.MAX_PURLS_PER_BATCH) {
    return purlsBodySchema(max).extend({
        page: zod_1.z.coerce.number().int().min(1).default(1),
        pageSize: zod_1.z.coerce.number().int().min(1).max(max).default(exports.DEFAULT_BATCH_PAGE_SIZE),
    });
}
// Slice the requested page out of a parsed paginatedPurlsBodySchema body and normalize
// only that page's purls, so a single page never resolves the whole batch.
function paginatePurls(body) {
    const { purls, page, pageSize } = body;
    const start = (page - 1) * pageSize;
    const pagedPurls = purls.slice(start, start + pageSize);
    return {
        page,
        pageSize,
        total: purls.length,
        pagedPurls,
        normalizedPurls: pagedPurls.map(normalizePurl),
    };
}
//# sourceMappingURL=purl.js.map