export interface MockPackageListItem {
  purl: string
  name: string
  ecosystem: string
  health: number
  impact: number
  lifecycle: 'active' | 'stable' | 'declining' | 'abandoned'
  maintainerBusFactor: number
  openVulns: { low: number; medium: number; high: number; critical: number }
  stewardship: string
  steward: null
}

export interface MockPackageDetail {
  purl: string
  name: string
  ecosystem: string
  general: {
    healthScore: { maintainerHealth: number; securitySupplyChain: number; developmentActivity: number; total: number }
    impact: { impactScore: number; downloadsLastMonth: number | null; dependentPackages: number; dependentRepos: number; transitiveReach: string }
    riskSignals: { lifecycle: string; maintainerBusFactor: number; lastRelease: string; hasSecurityFile: null; openSSFScorecard: number }
  }
  assessment: Record<string, never>
  security: {
    securityContacts: null
    advisories: Array<{ osvId: string; severity: 'critical' | 'high' | 'medium' | 'low'; resolution: null }>
    cvd: { isPvrEnabled: null; hasSecurityPolicyEnabled: null; tier0Steward: null; criticalVulnerabilityFlag: boolean }
  }
  provenance: {
    repositoryMapping: { declaredRepo: string; mappingConfidence: number; lastCommitAt: string }
    supplyChainIntegrity: { buildProvenance: null; signedReleases: null }
  }
  history: Record<string, never>
}

export const MOCK_PACKAGES: MockPackageListItem[] = [
  {
    purl: 'pkg:npm/lodash@4.17.21',
    name: 'lodash',
    ecosystem: 'npm',
    health: 18,
    impact: 71,
    lifecycle: 'declining',
    maintainerBusFactor: 1,
    openVulns: { low: 0, medium: 0, high: 1, critical: 0 },
    stewardship: 'unassigned',
    steward: null,
  },
  {
    purl: 'pkg:maven/org.apache.commons/commons-lang3@3.12.0',
    name: 'commons-lang3',
    ecosystem: 'maven',
    health: 62,
    impact: 88,
    lifecycle: 'stable',
    maintainerBusFactor: 3,
    openVulns: { low: 0, medium: 0, high: 0, critical: 0 },
    stewardship: 'unassigned',
    steward: null,
  },
  {
    purl: 'pkg:npm/minimist@1.2.6',
    name: 'minimist',
    ecosystem: 'npm',
    health: 12,
    impact: 95,
    lifecycle: 'abandoned',
    maintainerBusFactor: 1,
    openVulns: { low: 0, medium: 1, high: 0, critical: 1 },
    stewardship: 'unassigned',
    steward: null,
  },
]

export const MOCK_DETAILS: Record<string, MockPackageDetail> = {
  'pkg:npm/lodash@4.17.21': {
    purl: 'pkg:npm/lodash@4.17.21',
    name: 'lodash',
    ecosystem: 'npm',
    general: {
      healthScore: { maintainerHealth: 4, securitySupplyChain: 8, developmentActivity: 6, total: 18 },
      impact: { impactScore: 71, downloadsLastMonth: 52142891, dependentPackages: 142312, dependentRepos: 39104, transitiveReach: 'Top 0.4%' },
      riskSignals: { lifecycle: 'declining', maintainerBusFactor: 1, lastRelease: '2021-02-20T00:00:00Z', hasSecurityFile: null, openSSFScorecard: 5.2 },
    },
    assessment: {},
    security: {
      securityContacts: null,
      advisories: [{ osvId: 'CVE-2021-44906', severity: 'high', resolution: null }],
      cvd: { isPvrEnabled: null, hasSecurityPolicyEnabled: null, tier0Steward: null, criticalVulnerabilityFlag: false },
    },
    provenance: {
      repositoryMapping: { declaredRepo: 'https://github.com/lodash/lodash', mappingConfidence: 0.98, lastCommitAt: '2024-09-14T00:00:00Z' },
      supplyChainIntegrity: { buildProvenance: null, signedReleases: null },
    },
    history: {},
  },
  'pkg:maven/org.apache.commons/commons-lang3@3.12.0': {
    purl: 'pkg:maven/org.apache.commons/commons-lang3@3.12.0',
    name: 'commons-lang3',
    ecosystem: 'maven',
    general: {
      healthScore: { maintainerHealth: 18, securitySupplyChain: 22, developmentActivity: 22, total: 62 },
      impact: { impactScore: 88, downloadsLastMonth: null, dependentPackages: 89421, dependentRepos: 21033, transitiveReach: 'Top 1.2%' },
      riskSignals: { lifecycle: 'stable', maintainerBusFactor: 3, lastRelease: '2022-11-05T00:00:00Z', hasSecurityFile: null, openSSFScorecard: 7.1 },
    },
    assessment: {},
    security: {
      securityContacts: null,
      advisories: [],
      cvd: { isPvrEnabled: null, hasSecurityPolicyEnabled: null, tier0Steward: null, criticalVulnerabilityFlag: false },
    },
    provenance: {
      repositoryMapping: { declaredRepo: 'https://github.com/apache/commons-lang', mappingConfidence: 0.99, lastCommitAt: '2024-10-01T00:00:00Z' },
      supplyChainIntegrity: { buildProvenance: null, signedReleases: null },
    },
    history: {},
  },
  'pkg:npm/minimist@1.2.6': {
    purl: 'pkg:npm/minimist@1.2.6',
    name: 'minimist',
    ecosystem: 'npm',
    general: {
      healthScore: { maintainerHealth: 2, securitySupplyChain: 4, developmentActivity: 6, total: 12 },
      impact: { impactScore: 95, downloadsLastMonth: 102381944, dependentPackages: 321042, dependentRepos: 87231, transitiveReach: 'Top 0.1%' },
      riskSignals: { lifecycle: 'abandoned', maintainerBusFactor: 1, lastRelease: '2022-03-17T00:00:00Z', hasSecurityFile: null, openSSFScorecard: 2.1 },
    },
    assessment: {},
    security: {
      securityContacts: null,
      advisories: [
        { osvId: 'CVE-2021-44906', severity: 'critical', resolution: null },
        { osvId: 'CVE-2020-7598', severity: 'medium', resolution: null },
      ],
      cvd: { isPvrEnabled: null, hasSecurityPolicyEnabled: null, tier0Steward: null, criticalVulnerabilityFlag: true },
    },
    provenance: {
      repositoryMapping: { declaredRepo: 'https://github.com/minimistjs/minimist', mappingConfidence: 0.97, lastCommitAt: '2022-03-17T00:00:00Z' },
      supplyChainIntegrity: { buildProvenance: null, signedReleases: null },
    },
    history: {},
  },
}

export const MOCK_METRICS = {
  totalPackages: MOCK_PACKAGES.length,
  criticalPackages: MOCK_PACKAGES.filter((p) => p.openVulns.critical > 0).length,
}
