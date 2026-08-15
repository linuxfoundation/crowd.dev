"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.bodySchema = exports.canonicalizeSourceUrl = void 0;
const zod_1 = require("zod");
const permissions_1 = __importDefault(require("../../../security/permissions"));
const integrationService_1 = __importDefault(require("../../../services/integrationService"));
const permissionChecker_1 = __importDefault(require("../../../services/user/permissionChecker"));
const validation_1 = require("../../../utils/validation");
const MAX_LIST_NAME_LENGTH = 255;
// `name` is a display label only — the worker keys the mirror path by the
// DB-generated list id, not `name` (see mirror_service.py's
// list_mirror_dir/_validate_list_id), so it's free to contain path-like
// characters (lore lists are nested, e.g. "some-project/git"). Only reject
// the null byte, which breaks C-string-based tooling downstream regardless
// of context.
const isSafeListName = (name) => name.length > 0 && name.length <= MAX_LIST_NAME_LENGTH && !name.includes('\0');
// public-inbox-clone in the worker fetches this URL as-is; restrict to
// https so a caller can't point the worker at file://, javascript:, or a
// bare non-URL string. Requiring https already blocks the classic SSRF
// target (cloud-metadata IMDS is http-only, per securityTxt.ts precedent);
// also reject obvious loopback/localhost literals.
const isBlockedHost = (h) => h === 'localhost' || h === '::1' || h === '0.0.0.0' || h.startsWith('127.');
const isSafeSourceUrl = (sourceUrl) => {
    try {
        const url = new URL(sourceUrl);
        return (url.protocol === 'https:' &&
            !isBlockedHost(url.hostname.toLowerCase()) &&
            // Credentials/query/fragment have no meaning for a public-inbox archive
            // URL; rejecting them keeps canonicalizeSourceUrl a lossless
            // normalization instead of one that silently drops caller-supplied data.
            url.username === '' &&
            url.password === '' &&
            url.search === '' &&
            url.hash === '');
    }
    catch (_a) {
        return false;
    }
};
// "https://host/list" and "https://HOST/list/" and "https://host:443/list"
// must all resolve to the same DB row — scheme case, host case, the default
// https port, and a trailing slash are not meaningful differences for the
// same archive, but a plain trailing-slash strip left them as distinct
// strings, bypassing the cross-project ownership check and causing
// duplicate ingestion. The worker's ensure_mirror() already normalizes to
// this same form before cloning (mirror_service.py), so storage must match.
// Kept as a plain function (not a zod .transform()/.preprocess()) since
// either makes the field optional in z.infer with the installed zod v4 —
// reproduced in isolation, unrelated to this schema's nesting.
const canonicalizeSourceUrl = (sourceUrl) => {
    const url = new URL(sourceUrl);
    const port = url.port && url.port !== '443' ? `:${url.port}` : '';
    const path = url.pathname.replace(/\/+$/, '') || '/';
    return `https://${url.hostname.toLowerCase()}${port}${path}`;
};
exports.canonicalizeSourceUrl = canonicalizeSourceUrl;
exports.bodySchema = zod_1.z.object({
    lists: zod_1.z
        .array(zod_1.z.object({
        name: zod_1.z.string().trim().min(1).refine(isSafeListName, {
            message: 'Invalid mailing list name',
        }),
        sourceUrl: zod_1.z.string().trim().min(1).refine(isSafeSourceUrl, {
            message: 'sourceUrl must be a valid https:// URL',
        }),
    }))
        .min(1, 'lists must contain at least one mailing list')
        .refine((lists) => new Set(lists.map((l) => (0, exports.canonicalizeSourceUrl)(l.sourceUrl))).size === lists.length, { message: 'lists contains duplicate sourceUrl entries' }),
});
exports.default = async (req, res) => {
    new permissionChecker_1.default(req).validateHas(permissions_1.default.values.tenantEdit);
    const integrationData = (0, validation_1.validateOrThrow)(exports.bodySchema, req.body);
    integrationData.lists = integrationData.lists.map((l) => ({
        ...l,
        sourceUrl: (0, exports.canonicalizeSourceUrl)(l.sourceUrl),
    }));
    const payload = await new integrationService_1.default(req).mailingListConnectOrUpdate(integrationData);
    await req.responseHandler.success(req, res, payload);
};
//# sourceMappingURL=mailingListAuthenticate.js.map