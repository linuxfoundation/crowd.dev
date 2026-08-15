"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.alertOnce = alertOnce;
const crypto_1 = require("crypto");
const common_1 = require("@crowd/common");
const redis_1 = require("@crowd/redis");
const slack_1 = require("@crowd/slack");
const PATH_UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/gi;
async function alertOnce(req, { status, code, message, name, context, stack, }) {
    if (status !== 409 && status < 500)
        return;
    const path = (req.originalUrl || req.url || '').split('?')[0];
    // akrites alerts are handled separately, so skip them here.
    if (path.startsWith('/v1/akrites') || path.startsWith('/v1/akrites-external')) {
        return;
    }
    const route = resolveRoute(req);
    const dedupeKey = (0, crypto_1.createHash)('sha256')
        .update([status, req.method, route, code, message, serializeContext(context)]
        .filter((part) => part !== '')
        .join(':'))
        .digest('hex');
    const cache = new redis_1.RedisCache('public-api-alerts', req.redis, req.log);
    const lease = (0, common_1.generateUUIDv4)();
    try {
        const held = await cache.setIfNotExistsOrGet(dedupeKey, lease, 60 * 60);
        if (held !== lease) {
            req.log.info({ dedupeKey }, 'Skipping duplicate public API alert');
            return;
        }
    }
    catch (err) {
        req.log.warn({ err, dedupeKey }, 'Alert dedupe failed; sending anyway');
    }
    const sections = [
        {
            title: 'Request',
            text: `*Method:* \`${req.method}\`\n*URL:* \`${req.originalUrl || req.url}\``,
        },
        {
            title: 'Error',
            text: `*Code:* \`${code}\`\n*Name:* \`${name || code}\`\n*Message:* ${message}`,
        },
    ];
    if (context && Object.keys(context).length > 0) {
        sections.push({
            title: 'Context',
            text: `\`\`\`${JSON.stringify(context, null, 2)}\`\`\``,
        });
    }
    if (stack) {
        sections.push({
            title: 'Stack Trace',
            text: `\`\`\`${stack.substring(0, 2700)}\`\`\``,
        });
    }
    (0, slack_1.sendSlackNotification)(slack_1.SlackChannel.CDP_PUBLIC_API_ALERTS, status >= 500 ? slack_1.SlackPersona.ERROR_REPORTER : slack_1.SlackPersona.WARNING_PROPAGATOR, status >= 500 ? `500 Error: ${name || message}` : `${status} Conflict: ${message}`, sections);
}
function resolveRoute(req) {
    return (req.originalUrl || req.url || '').split('?')[0].replace(PATH_UUID, ':id');
}
function serializeContext(context) {
    if (!context)
        return '';
    const normalized = {};
    for (const key of Object.keys(context).sort()) {
        const value = context[key];
        normalized[key] = Array.isArray(value) ? [...value].map(String).sort() : value;
    }
    return JSON.stringify(normalized);
}
//# sourceMappingURL=alertOnce.js.map