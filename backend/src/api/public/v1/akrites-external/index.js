"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.akritesExternalRouter = akritesExternalRouter;
const express_1 = require("express");
const apiRateLimiter_1 = require("@/api/apiRateLimiter");
const requireScopes_1 = require("@/api/public/middlewares/requireScopes");
const errorMiddleware_1 = require("@/middlewares/errorMiddleware");
const scopes_1 = require("@/security/scopes");
const getAkritesExternalAdvisoryDetail_1 = require("../packages/getAkritesExternalAdvisoryDetail");
const getAkritesExternalAdvisoryDetailBatch_1 = require("../packages/getAkritesExternalAdvisoryDetailBatch");
const getAkritesExternalContactDetail_1 = require("../packages/getAkritesExternalContactDetail");
const getAkritesExternalContactDetailBatch_1 = require("../packages/getAkritesExternalContactDetailBatch");
const getAkritesExternalPackageDetail_1 = require("../packages/getAkritesExternalPackageDetail");
const getAkritesExternalPackageDetailBatch_1 = require("../packages/getAkritesExternalPackageDetailBatch");
const getAkritesExternalProjectProfiling_1 = require("../packages/getAkritesExternalProjectProfiling");
const getAkritesExternalProjectProfilingBatch_1 = require("../packages/getAkritesExternalProjectProfilingBatch");
const getBlastRadiusJob_1 = require("../packages/getBlastRadiusJob");
const getBlastRadiusJobBatch_1 = require("../packages/getBlastRadiusJobBatch");
const ingestAkritesExternalContactDetail_1 = require("../packages/ingestAkritesExternalContactDetail");
const submitBlastRadiusJob_1 = require("../packages/submitBlastRadiusJob");
const submitBlastRadiusJobBatch_1 = require("../packages/submitBlastRadiusJobBatch");
const rateLimiter = (0, apiRateLimiter_1.createRateLimiter)({ max: 60, windowMs: 60 * 1000 });
// Shared by every endpoint below that kicks off a Temporal workflow per request — those
// get their own, much stricter limiter than plain reads, configurable via env so it can
// be tuned without a redeploy.
function envTunableRateLimiter(envPrefix, defaultMax, defaultWindowMs) {
    const max = Number(process.env[`${envPrefix}_MAX`]);
    const windowMs = Number(process.env[`${envPrefix}_WINDOW_MS`]);
    return (0, apiRateLimiter_1.createRateLimiter)({
        max: Number.isSafeInteger(max) && max > 0 ? max : defaultMax,
        windowMs: Number.isSafeInteger(windowMs) && windowMs > 0 ? windowMs : defaultWindowMs,
    });
}
// Blast-radius jobs default to 50 requests/hour.
const blastRadiusRateLimiter = envTunableRateLimiter('AKRITES_BLAST_RADIUS_RATE_LIMIT', 5, 60 * 60 * 1000);
// /contacts/ingest starts a Temporal workflow and blocks for it (worst case ~95s per
// attempt cycle, plus unbounded time waiting for a free worker slot — see
// security-contacts/workflows.ts's singleActs config), vs. the read-only /contacts/detail
// endpoints, so it gets its own limiter. Defaults to 20 requests/hour.
const contactIngestRateLimiter = envTunableRateLimiter('AKRITES_CONTACT_INGEST_RATE_LIMIT', 20, 60 * 60 * 1000);
function akritesExternalRouter() {
    const router = (0, express_1.Router)();
    // Any one of the dedicated Akrites scope or the old Self Serve scopes works for now —
    // drop READ_PACKAGES/READ_STEWARDSHIPS once Akrites cuts over.
    const packagesSubRouter = (0, express_1.Router)();
    packagesSubRouter.use(rateLimiter);
    packagesSubRouter.use((0, requireScopes_1.requireScopes)([scopes_1.SCOPES.READ_AKRITES_PACKAGES, scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_STEWARDSHIPS], 'any'));
    packagesSubRouter.get('/detail', (0, errorMiddleware_1.safeWrap)(getAkritesExternalPackageDetail_1.getAkritesExternalPackageDetail));
    packagesSubRouter.post(/^\/detail:batch\/?$/, (0, errorMiddleware_1.safeWrap)(getAkritesExternalPackageDetailBatch_1.getAkritesExternalPackageDetailBatch));
    router.use('/packages', packagesSubRouter);
    // Assembled protocol methods include inferred security_contacts fallbacks whose
    // endpoint can be a maintainer/committer email (contact PII), so these routes ride
    // the same maintainer scopes as /contacts, never the packages scopes.
    const projectProfilingScopes = [scopes_1.SCOPES.READ_MAINTAINER_ROLES, scopes_1.SCOPES.READ_AKRITES_MAINTAINERS];
    router.get('/project-profiling', rateLimiter, (0, requireScopes_1.requireScopes)(projectProfilingScopes, 'any'), (0, errorMiddleware_1.safeWrap)(getAkritesExternalProjectProfiling_1.getAkritesExternalProjectProfiling));
    router.post(/^\/project-profiling:batch\/?$/, rateLimiter, (0, requireScopes_1.requireScopes)(projectProfilingScopes, 'any'), (0, errorMiddleware_1.safeWrap)(getAkritesExternalProjectProfilingBatch_1.getAkritesExternalProjectProfilingBatch));
    // Dedicated read:akrites-advisories, or Self Serve's read:packages as a
    // fallback until Akrites cuts over — drop it then.
    const advisoriesScopes = [scopes_1.SCOPES.READ_PACKAGES, scopes_1.SCOPES.READ_AKRITES_ADVISORIES];
    const advisoriesSubRouter = (0, express_1.Router)();
    advisoriesSubRouter.use(rateLimiter);
    advisoriesSubRouter.use((0, requireScopes_1.requireScopes)(advisoriesScopes, 'any'));
    advisoriesSubRouter.get('/detail', (0, errorMiddleware_1.safeWrap)(getAkritesExternalAdvisoryDetail_1.getAkritesExternalAdvisoryDetail));
    advisoriesSubRouter.post(/^\/detail:batch\/?$/, (0, errorMiddleware_1.safeWrap)(getAkritesExternalAdvisoryDetailBatch_1.getAkritesExternalAdvisoryDetailBatch));
    router.use('/advisories', advisoriesSubRouter);
    // Contact PII stays behind a dedicated scope, never the packages scope: dedicated
    // read:akrites-maintainers, or Self Serve's read:maintainer-roles as a fallback.
    //
    // requireScopes is applied per-route (not router-level) so each route can put its own
    // rate limiter *before* the scope check — failed-auth requests still count against that
    // route's quota — without forcing every route in this subrouter onto the same limiter
    // instance. /ingest gets its own dedicated contactIngestRateLimiter instead of sharing
    // the read endpoints' quota, matching the blast-radius jobs endpoint below.
    const contactsScopes = [scopes_1.SCOPES.READ_MAINTAINER_ROLES, scopes_1.SCOPES.READ_AKRITES_MAINTAINERS];
    const contactsSubRouter = (0, express_1.Router)();
    contactsSubRouter.get('/detail', rateLimiter, (0, requireScopes_1.requireScopes)(contactsScopes, 'any'), (0, errorMiddleware_1.safeWrap)(getAkritesExternalContactDetail_1.getAkritesExternalContactDetail));
    contactsSubRouter.post(/^\/detail:batch\/?$/, rateLimiter, (0, requireScopes_1.requireScopes)(contactsScopes, 'any'), (0, errorMiddleware_1.safeWrap)(getAkritesExternalContactDetailBatch_1.getAkritesExternalContactDetailBatch));
    // Sync, single-purl on-demand ingest — starts a Temporal workflow and blocks a while,
    // so it gets the dedicated contactIngestRateLimiter, not the shared rateLimiter above.
    contactsSubRouter.post('/ingest', contactIngestRateLimiter, (0, requireScopes_1.requireScopes)(contactsScopes, 'any'), (0, errorMiddleware_1.safeWrap)(ingestAkritesExternalContactDetail_1.ingestAkritesExternalContactDetail));
    router.use('/contacts', contactsSubRouter);
    // Same underlying data as advisories above, same scopes: read:akrites-advisories,
    // or Self Serve's read:packages as a fallback until Akrites cuts over.
    const blastRadiusSubRouter = (0, express_1.Router)();
    blastRadiusSubRouter.use((0, requireScopes_1.requireScopes)(advisoriesScopes, 'any'));
    blastRadiusSubRouter.post('/jobs', blastRadiusRateLimiter, (0, errorMiddleware_1.safeWrap)(submitBlastRadiusJob_1.submitBlastRadiusJob));
    // Bulk submit multiplies Temporal workflow starts per request (up to
    // MAX_BLAST_RADIUS_JOBS_PER_BATCH), so it sits behind the same strict
    // blastRadiusRateLimiter as the single-job route, not the regular one.
    blastRadiusSubRouter.post(/^\/jobs:batch\/?$/, blastRadiusRateLimiter, (0, errorMiddleware_1.safeWrap)(submitBlastRadiusJobBatch_1.submitBlastRadiusJobBatch));
    blastRadiusSubRouter.get('/jobs/:analysisId', rateLimiter, (0, errorMiddleware_1.safeWrap)(getBlastRadiusJob_1.getBlastRadiusJob));
    // Bulk poll is read-only, same cost profile as the other batch endpoints, so
    // it uses the regular rateLimiter.
    blastRadiusSubRouter.post(/^\/jobs:batch\/poll\/?$/, rateLimiter, (0, errorMiddleware_1.safeWrap)(getBlastRadiusJobBatch_1.getBlastRadiusJobBatch));
    router.use('/blast-radius', blastRadiusSubRouter);
    return router;
}
//# sourceMappingURL=index.js.map