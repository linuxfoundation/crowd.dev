"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.toBlastRadiusAnalysis = toBlastRadiusAnalysis;
// Crosswalk from the DB's NUMERIC(3,2) 0-1 confidence score to the contract's
// enum — matches the PoC methodology's confidence bands (report.py).
function toResultConfidence(confidence) {
    if (confidence >= 0.8)
        return 'high';
    if (confidence >= 0.4)
        return 'medium';
    return 'low';
}
// d.name shape varies by ecosystem: npm/go store a plain name, maven stores
// "groupId:artifactId" — split on the first colon only, artifactId may itself contain one.
//
// Scoped npm names (@scope/name) need their leading @ percent-encoded to %40 —
// matching both purl.ts's normalizePurl and the contract's own example response
// bodies (e.g. pkg:npm/%40angular/core in openapi.yaml).
function toPurl(ecosystem, name) {
    switch (ecosystem) {
        case 'maven': {
            const colon = name.indexOf(':');
            if (colon === -1)
                return `pkg:maven/${name}`;
            return `pkg:maven/${name.slice(0, colon)}/${name.slice(colon + 1)}`;
        }
        case 'go':
            return `pkg:golang/${name}`;
        case 'cargo':
            return `pkg:cargo/${name}`;
        case 'npm':
        default:
            return `pkg:npm/${name.replace(/^@/, '%40')}`;
    }
}
function flattenEvidence(evidence) {
    if (!evidence || evidence.length === 0)
        return null;
    return evidence
        .map((e) => {
        const file = e.file ? String(e.file) : null;
        const line = e.line !== undefined && e.line !== null ? String(e.line) : null;
        const snippet = e.snippet ? String(e.snippet) : null;
        const location = [file, line].filter(Boolean).join(':');
        return [location, snippet].filter(Boolean).join(' — ');
    })
        .filter(Boolean)
        .join('\n');
}
// reachable_verdict is 'affected' | 'not_affected' | 'unclear' (see VERDICT_SCHEMA in
// agent/prompts.ts) — 'unclear' covers both a genuine ambiguous read and a persistent
// agent failure (upsertErrorVerdict in reachability.ts). affected=false alone can't
// distinguish "confirmed not affected" from "we don't know" — expose the raw verdict
// too so consumers who care can tell the difference.
function toVerdict(reachableVerdict) {
    if (reachableVerdict === 'affected' || reachableVerdict === 'not_affected') {
        return reachableVerdict;
    }
    return 'unclear';
}
function toResultItem(ecosystem, row) {
    const verdict = toVerdict(row.reachable_verdict);
    return {
        dependent: toPurl(ecosystem, row.name),
        affected: verdict === 'affected',
        verdict,
        confidence: toResultConfidence(row.confidence),
        evidence: flattenEvidence(row.evidence),
        downloadsLast30Days: row.downloads !== null ? String(row.downloads) : null,
    };
}
// Population-level summary. dependentsExcludedUpfront comes from the
// blast_radius_dependents rows actually marked excluded_by_range=true — NOT from
// candidates_considered, which is stage 2's phase-1 population and also counts
// candidates the topN walk never reached (so candidatesConsidered - analyzed
// overstates range exclusions). dependentsAnalyzed = number of verdicts produced,
// dependentsAffected = count with an 'affected' verdict, affectedPercentage rounded
// to 1 decimal — computed over conclusive verdicts only (affected/not_affected), so
// persistent agent/tarball failures ('unclear') don't drag it toward a misleading 0%.
// null when nothing conclusive was analyzed.
function toSummary(dependentsExcludedUpfront, results) {
    const dependentsAnalyzed = results.length;
    const affected = results.filter((r) => r.affected);
    const conclusive = results.filter((r) => r.verdict !== 'unclear');
    return {
        totalDependentsInRange: dependentsAnalyzed + dependentsExcludedUpfront,
        dependentsExcludedUpfront,
        dependentsAnalyzed,
        dependentsAffected: affected.length,
        affectedPercentage: conclusive.length > 0 ? Math.round((affected.length / conclusive.length) * 1000) / 10 : null,
        affectedDependents: affected.map((r) => r.dependent),
    };
}
function toBlastRadiusAnalysis(analysis, verdictRows, dependentsExcludedByRangeCount) {
    const status = analysis.status;
    const done = status === 'done';
    const ecosystem = analysis.ecosystem;
    const results = done ? verdictRows.map((row) => toResultItem(ecosystem, row)) : null;
    return {
        analysisId: analysis.id,
        status,
        advisoryId: analysis.advisory_osv_id,
        package: analysis.package_name,
        ecosystem,
        submittedAt: analysis.started_at,
        completedAt: analysis.completed_at,
        errorMessage: analysis.error,
        summary: done && results ? toSummary(dependentsExcludedByRangeCount, results) : null,
        results,
    };
}
//# sourceMappingURL=blastRadiusAnalysis.js.map