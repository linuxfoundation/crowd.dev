import type { Request, Response } from 'express'
import { z } from 'zod'

import { NotFoundError } from '@crowd/common'

import { ok } from '@/utils/api'
import { validateOrThrow } from '@/utils/validation'

const paramsSchema = z.object({
  purl: z.string().trim().min(1),
})

// TODO: replace with real DB queries once stewardship tables land
export async function getPackage(req: Request, res: Response): Promise<void> {
  const { purl: rawPurl } = validateOrThrow(paramsSchema, req.params)
  const purl = decodeURIComponent(rawPurl)

  if (!purl.startsWith('pkg:')) {
    throw new NotFoundError()
  }

  ok(res, mockPackage(purl))
}

function mockPackage(purl: string) {
  const name = purl.split('/').pop()?.split('@')[0] ?? purl
  const ecosystem = purl.startsWith('pkg:npm') ? 'npm' : purl.startsWith('pkg:maven') ? 'maven' : 'unknown'

  return {
    purl,
    name,
    ecosystem,
    latestVersion: '4.17.21',
    latestReleaseAt: '2021-02-20T00:00:00Z',
    downloads: ecosystem === 'maven' ? null : 52142891,
    dependentPackagesCount: 142312,
    dependentReposCount: 39104,
    lifecycle: 'declining',
    health: 18,
    healthBreakdown: {
      maintainerHealth: 4,
      securityAndSupplyChain: 8,
      developmentActivity: 6,
    },
    impact: 71,
    transitiveReach: 'Top 0.4%',
    busFactor: 1,
    openVulns: { count: 1, severity: 'high' },
    repository: {
      url: `https://github.com/example/${name}`,
      lastCommitAt: '2024-09-14T00:00:00Z',
      scorecardScore: 5.2,
      isArchived: false,
    },
    maintainers: [
      {
        username: 'jdalton',
        displayName: 'John-David Dalton',
        emailHash: null,
        url: 'https://github.com/jdalton',
      },
    ],
    securityContact: null,
    advisories: [
      {
        id: 'CVE-2021-44906',
        severity: 'high',
        summary: 'Prototype pollution via constructor',
        status: 'open',
        cvss: 9.8,
        publishedAt: '2022-03-17T00:00:00Z',
        affectedVersionRange: {
          introduced: '0.0.1',
          fixed: null,
          lastAffected: '4.17.21',
        },
      },
    ],
    scorecardChecks: [
      { checkName: 'Branch-Protection', score: 3.0, reason: 'Branch protection not enabled for default branch' },
      { checkName: 'Security-Policy', score: 0.0, reason: 'security policy file not detected' },
      { checkName: 'Maintained', score: 5.0, reason: 'repo was created 9 years ago, has 337 open issues' },
    ],
    disclosureReadiness: {
      pvrEnabled: null,
      securityMdPresent: null,
      tier0StewardName: null,
      hasCriticalAdvisory: true,
    },
    provenanceMappings: [
      {
        repoUrl: `https://github.com/example/${name}`,
        confidence: 0.98,
        source: 'declared',
        verified: true,
      },
    ],
    supplyChainIntegrity: {
      buildProvenance: null,
      signedReleases: null,
    },
    stewardship: {
      status: 'escalated',
      origin: 'auto_imported',
      stewards: [
        { userId: 'jdoe', name: 'Jonathan R.', role: 'lead', assignedAt: '2025-05-15T00:00:00Z' },
      ],
      lastActivityAt: '2025-06-08T00:00:00Z',
      lastActivityDescription: 'Escalated — recommending consortium fork',
      assessment: {
        posture: 'Critical',
        summary: 'Single maintainer, bus factor 1, no security process in place.',
        draft: false,
        reviewed: false,
        flagged: false,
        flagNote: null,
        monitoringPlan: 'Watch for new maintainer activity\nWatch registry for ownership transfer',
        completedAt: '2025-05-18T00:00:00Z',
        completedBy: 'jdoe',
      },
      findings: [
        {
          id: 1,
          dimension: 'maintainer_health',
          severity: 'critical',
          finding: 'Single maintainer, no response to issue filed 6 weeks ago. Bus factor 1.',
          evidence: null,
        },
        {
          id: 2,
          dimension: 'release_health',
          severity: 'high',
          finding: 'Last release 5 years ago. 180+ open issues. Active community forks exist.',
          evidence: null,
        },
        {
          id: 3,
          dimension: 'security_posture',
          severity: 'high',
          finding: 'No SECURITY.md, no disclosure process, releases unsigned.',
          evidence: null,
        },
      ],
      remediationActions: [
        {
          id: 1,
          findingId: 3,
          action: 'Filed issue requesting SECURITY.md + disclosure process (no response)',
          status: 'done',
          url: null,
          notes: null,
          completedAt: '2025-05-20T00:00:00Z',
        },
        {
          id: 2,
          findingId: null,
          action: 'Opened PR re-enabling branch protection',
          status: 'done',
          url: null,
          notes: null,
          completedAt: '2025-05-22T00:00:00Z',
        },
        {
          id: 3,
          findingId: null,
          action: 'Recommended escalation — vendor LTS or consortium fork',
          status: 'pending',
          url: null,
          notes: null,
          completedAt: null,
        },
      ],
      activity: [
        {
          id: 4,
          actorUserId: 'jdoe',
          actorType: 'user',
          activityType: 'escalation',
          content: 'Escalated — recommending consortium fork',
          metadata: { from: 'active', to: 'escalated' },
          createdAt: new Date(Date.now() - 1 * 24 * 60 * 60 * 1000).toISOString(),
        },
        {
          id: 3,
          actorUserId: 'jdoe',
          actorType: 'user',
          activityType: 'remediation_logged',
          content: 'Jonathan R. logged: maintainer unresponsive after 3 attempts',
          metadata: null,
          createdAt: new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString(),
        },
        {
          id: 2,
          actorUserId: 'jdoe',
          actorType: 'user',
          activityType: 'assessment_completed',
          content: 'Assessment completed — posture Critical',
          metadata: { posture: 'Critical' },
          createdAt: new Date(Date.now() - 21 * 24 * 60 * 60 * 1000).toISOString(),
        },
        {
          id: 1,
          actorUserId: null,
          actorType: 'system',
          activityType: 'steward_added',
          content: 'Assigned to Jonathan R.',
          metadata: { userId: 'jdoe', role: 'lead' },
          createdAt: new Date(Date.now() - 24 * 24 * 60 * 60 * 1000).toISOString(),
        },
      ],
    },
  }
}
