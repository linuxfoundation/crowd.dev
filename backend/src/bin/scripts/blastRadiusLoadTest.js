"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
// Local load-test harness for the blast-radius Temporal pipeline. Not meant to
// ship — this is a dev-only tool, kept under bin/scripts like other one-offs.
//
// Starts N real analyzeBlastRadius workflows directly via the Temporal client
// (bypassing the public HTTP/Zod/Auth0 layer, same as prod's own submit path
// underneath), optionally capping the worker container's memory and stopping
// each workflow right after a chosen stage via `stopAfterStage` — so this can
// safely profile stage 2 (dependents, no LLM) without ever reaching the paid
// stage 3 (reachability, Sonnet) unless explicitly asked to.
//
// Usage:
//   cd backend && npx tsx src/bin/scripts/blastRadiusLoadTest.ts \
//     --jobs=8 --scanConcurrency=8 --memCap=2g --stopAfter=dependents \
//     --advisories=src/bin/scripts/blastRadiusLoadTestAdvisories.json
//
// Flags (all optional):
//   --jobs=N            number of concurrent analyses to start (default 8)
//   --scanConcurrency=N sets BLAST_RADIUS_SCAN_CONCURRENCY on the worker container
//                        for the duration of this run, then clears it (default: unset)
//   --memCap=2g          docker memory cap applied to the worker container for the
//                        duration of this run, then reset to unlimited (default: none)
//   --stopAfter=STAGE    'intel' | 'dependents' | 'reachability' — workflow stops
//                        right after this stage succeeds (default: 'dependents')
//   --container=NAME     worker container name (default crowd_blast-radius-worker-dev_1)
//   --advisories=FILE    path to a JSON array of {advisoryId, package, ecosystem} —
//                        jobs round-robin across these instead of all hitting the
//                        same package (default: a single lodash advisory, repeated)
const child_process_1 = require("child_process");
const fs = __importStar(require("fs"));
const common_1 = require("@crowd/common");
const blastRadiusDal = __importStar(require("@crowd/data-access-layer/src/packages/blastRadius"));
const types_1 = require("@crowd/types");
const packagesDb_1 = require("@/db/packagesDb");
const packagesTemporal_1 = require("@/db/packagesTemporal");
function flag(name, fallback) {
    const arg = process.argv.find((a) => a.startsWith(`--${name}=`));
    return arg ? arg.slice(name.length + 3) : fallback;
}
const JOBS = Number(flag('jobs', '8'));
const SCAN_CONCURRENCY = flag('scanConcurrency');
const MEM_CAP = flag('memCap');
const STOP_AFTER = flag('stopAfter', 'dependents');
const CONTAINER = flag('container', 'crowd_blast-radius-worker-dev_1');
const ADVISORIES_FILE = flag('advisories');
const POLL_INTERVAL_MS = 5000;
const TIMEOUT_MS = 60 * 60 * 1000;
const DEFAULT_ADVISORIES = [
    { advisoryId: 'GHSA-jf85-cpcp-j695', package: 'lodash', ecosystem: 'npm' }, // real OSV.dev entry, validated
];
function loadAdvisories() {
    if (!ADVISORIES_FILE)
        return DEFAULT_ADVISORIES;
    const parsed = JSON.parse(fs.readFileSync(ADVISORIES_FILE, 'utf-8'));
    if (!Array.isArray(parsed) || parsed.length === 0) {
        throw new Error(`--advisories file must contain a non-empty JSON array: ${ADVISORIES_FILE}`);
    }
    return parsed;
}
function sh(cmd) {
    return (0, child_process_1.execSync)(cmd, { encoding: 'utf-8' }).trim();
}
function applyRunConfig() {
    if (MEM_CAP) {
        console.log(`[loadtest] capping ${CONTAINER} memory at ${MEM_CAP}`);
        sh(`docker update --memory=${MEM_CAP} --memory-swap=${MEM_CAP} ${CONTAINER}`);
    }
    if (SCAN_CONCURRENCY) {
        // Env vars can't be changed on an already-running container (unlike the memory
        // cgroup cap above, which docker update can patch live) — they're baked in at
        // container start. Verify the worker already has the value this run wants
        // instead of silently testing against whatever it happened to start with.
        const actual = sh(`docker exec ${CONTAINER} sh -c 'echo $BLAST_RADIUS_SCAN_CONCURRENCY'`);
        if (actual !== SCAN_CONCURRENCY) {
            throw new Error(`--scanConcurrency=${SCAN_CONCURRENCY} requested but ${CONTAINER} was started with ` +
                `BLAST_RADIUS_SCAN_CONCURRENCY=${actual || '(unset)'}. Restart it first: ` +
                `BLAST_RADIUS_SCAN_CONCURRENCY=${SCAN_CONCURRENCY} ./scripts/cli service blast-radius-worker restart`);
        }
        console.log(`[loadtest] confirmed BLAST_RADIUS_SCAN_CONCURRENCY=${SCAN_CONCURRENCY} on worker`);
    }
}
function resetRunConfig() {
    console.log('[loadtest] resetting container memory cap to unlimited');
    try {
        sh(`docker update --memory=0 --memory-swap=0 ${CONTAINER}`);
    }
    catch (_a) {
        // some docker versions reject 0; fall back to a generous cap instead of leaving 2g stuck
        try {
            sh(`docker update --memory=8g --memory-swap=8g ${CONTAINER}`);
        }
        catch (_b) {
            console.warn('[loadtest] could not reset memory cap automatically — check manually');
        }
    }
}
function sampleContainerStats() {
    try {
        const raw = sh(`docker stats ${CONTAINER} --no-stream --format "{{.MemUsage}}|{{.CPUPerc}}"`);
        const [memUsage, cpuPerc] = raw.split('|');
        return { memUsage, cpuPerc };
    }
    catch (_a) {
        return null;
    }
}
function percentile(values, p) {
    if (values.length === 0)
        return null;
    const sorted = [...values].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return sorted[idx];
}
function stats(values) {
    if (values.length === 0)
        return null;
    return {
        count: values.length,
        min: Math.min(...values),
        avg: Math.round(values.reduce((a, b) => a + b, 0) / values.length),
        p95: percentile(values, 95),
        max: Math.max(...values),
    };
}
async function main() {
    const advisories = loadAdvisories();
    console.log(`[loadtest] jobs=${JOBS} scanConcurrency=${SCAN_CONCURRENCY !== null && SCAN_CONCURRENCY !== void 0 ? SCAN_CONCURRENCY : '(default 32)'} ` +
        `memCap=${MEM_CAP !== null && MEM_CAP !== void 0 ? MEM_CAP : '(none)'} stopAfter=${STOP_AFTER} container=${CONTAINER} ` +
        `advisories=${advisories.length}${ADVISORIES_FILE ? ` (${ADVISORIES_FILE})` : ' (default)'}`);
    applyRunConfig();
    const qx = await (0, packagesDb_1.getPackagesQx)();
    const packagesTemporal = await (0, packagesTemporal_1.getPackagesTemporalClient)();
    const analysisIds = [];
    const submittedAt = Date.now();
    try {
        await Promise.all(Array.from({ length: JOBS }, async (_, i) => {
            const target = advisories[i % advisories.length];
            const analysisId = (0, common_1.generateUUIDv4)();
            const analysisInput = {
                id: analysisId,
                advisoryOsvId: target.advisoryId,
                packageName: target.package,
                ecosystem: target.ecosystem,
                force: false,
            };
            await blastRadiusDal.createAnalysis(qx, analysisInput);
            await packagesTemporal.workflow.start('analyzeBlastRadius', {
                taskQueue: 'blast-radius-worker',
                workflowId: `${types_1.TemporalWorkflowId.BLAST_RADIUS_ANALYSIS}/${analysisId}`,
                retry: { maximumAttempts: 1 },
                args: [
                    {
                        analysisId,
                        advisoryId: target.advisoryId,
                        package: target.package,
                        ecosystem: target.ecosystem,
                        force: false,
                        stopAfterStage: STOP_AFTER,
                    },
                ],
            });
            analysisIds.push(analysisId);
        }));
        console.log(`[loadtest] submitted ${analysisIds.length} analyses in ${Date.now() - submittedAt}ms`);
        console.log(`[loadtest] analysisIds: ${analysisIds.join(', ')}`);
        const deadline = Date.now() + TIMEOUT_MS;
        let allDone = false;
        const memSamples = [];
        while (Date.now() < deadline) {
            // With stopAfterStage, the analysis row itself stays 'running' forever (the
            // workflow returns cleanly instead of finishing all stages) — so completion
            // is judged from stage_runs reaching the requested stop stage, not from
            // blast_radius_analyses.status.
            const rows = await qx.select(`select analysis_id, status from blast_radius_stage_runs
         where analysis_id in ($(ids:csv)) and stage = $(stage)`, { ids: analysisIds, stage: STOP_AFTER });
            const finished = rows.filter((r) => r.status === 'succeeded' || r.status === 'failed');
            const sample = sampleContainerStats();
            if (sample) {
                memSamples.push(sample.memUsage);
                console.log(`[loadtest] ${finished.length}/${analysisIds.length} finished stage=${STOP_AFTER} ` +
                    `mem=${sample.memUsage} cpu=${sample.cpuPerc} (${new Date().toISOString()})`);
            }
            else {
                console.log(`[loadtest] ${finished.length}/${analysisIds.length} finished stage=${STOP_AFTER} ` +
                    `(${new Date().toISOString()})`);
            }
            if (finished.length === analysisIds.length) {
                allDone = true;
                break;
            }
            await new Promise((resolve) => {
                setTimeout(resolve, POLL_INTERVAL_MS);
            });
        }
        if (!allDone) {
            console.warn('[loadtest] TIMED OUT waiting for all analyses to reach the stop stage');
        }
        const stageRuns = await qx.select(`select analysis_id, stage, status, duration_ms, cost_usd, error
       from blast_radius_stage_runs where analysis_id in ($(ids:csv))`, { ids: analysisIds });
        console.log('\n=== Per-analysis stage outcomes ===');
        for (const id of analysisIds) {
            const runs = stageRuns.filter((r) => r.analysis_id === id);
            const summary = runs
                .map((r) => `${r.stage}=${r.status}${r.error ? ` (${r.error})` : ''}`)
                .join(', ');
            console.log(`${id}: ${summary || 'no stage runs recorded'}`);
        }
        console.log('\n=== Stage duration stats (ms) ===');
        for (const stage of ['intel', 'dependents', 'reachability', 'report']) {
            const durations = stageRuns
                .filter((r) => r.stage === stage && r.status === 'succeeded')
                .map((r) => Number(r.duration_ms));
            console.log(`${stage}:`, stats(durations));
        }
        // blast_radius_analyses.total_cost_usd is only ever set by the report stage
        // (finalizeAnalysis) — with stopAfterStage set, report never runs, so the only
        // place real per-stage cost shows up is here, on stage_runs, scoped to this run's
        // own analysisIds (not a global window, since other runs/deployments write here too).
        console.log('\n=== Cost (USD, from blast_radius_stage_runs) ===');
        let totalCost = 0;
        for (const stage of ['intel', 'dependents', 'reachability', 'report']) {
            const costs = stageRuns
                .filter((r) => r.stage === stage)
                .map((r) => { var _a; return Number((_a = r.cost_usd) !== null && _a !== void 0 ? _a : 0); });
            const stageCost = costs.reduce((sum, c) => sum + c, 0);
            totalCost += stageCost;
            console.log(`${stage}: $${stageCost.toFixed(4)} (${costs.length} runs)`);
        }
        console.log(`total: $${totalCost.toFixed(4)}`);
        if (memSamples.length > 0) {
            console.log(`\n=== Memory samples (docker stats, ${memSamples.length} points) ===`);
            console.log(memSamples.join(' -> '));
        }
        console.log(`\n[loadtest] total wall-clock: ${Date.now() - submittedAt}ms`);
        process.exit(allDone ? 0 : 1);
    }
    finally {
        resetRunConfig();
    }
}
main().catch((err) => {
    console.error('[loadtest] fatal error', err);
    resetRunConfig();
    process.exit(1);
});
//# sourceMappingURL=blastRadiusLoadTest.js.map