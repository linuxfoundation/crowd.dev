"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.segmentMiddleware = segmentMiddleware;
const segmentRepository_1 = __importDefault(require("../database/repositories/segmentRepository"));
/** Resolves segment(s) from the request and sets `req.currentSegments` for downstream handlers. */
async function segmentMiddleware(req, _res, next) {
    var _a;
    try {
        const options = req;
        const segmentRepository = new segmentRepository_1.default(options);
        const querySegments = toStringArray(req.query.segments);
        const bodySegments = toStringArray((_a = req.body) === null || _a === void 0 ? void 0 : _a.segments);
        const segmentIds = querySegments.length > 0 ? querySegments : bodySegments;
        if (segmentIds.length > 0) {
            options.currentSegments = await segmentRepository.findInIds(segmentIds);
        }
        else {
            const { rows } = await segmentRepository.querySubprojects({ limit: 1, offset: 0 });
            options.currentSegments = rows;
        }
        next();
    }
    catch (error) {
        next(error);
    }
}
/**
 * Safely extracts a string[] from an unknown query/body value.
 */
function toStringArray(value) {
    if (value === undefined || value === null)
        return [];
    const items = Array.isArray(value) ? value : [value];
    return items
        .filter((item) => typeof item === 'string')
        .map((item) => item.trim())
        .filter(Boolean);
}
//# sourceMappingURL=segmentMiddleware.js.map