import { ClassificationNode } from '../activities/requestClassificationTrace'

export interface IClassificationCase {
  id: string
  description: string
  expectedNodes: ClassificationNode[]
  repoUrls: string[]
  text: string
}

export const CLASSIFICATION_CASES: IClassificationCase[] = [
  {
    id: 'non-lf-single-repo',
    description: 'A single repo of a non-LF project',
    expectedNodes: ['non_lf_create_in_external_group'],
    repoUrls: ['https://github.com/posit-dev/great-docs'],
    text: [
      'Onboard Great Docs',
      'Project name: Great Docs\nRepository: https://github.com/posit-dev/great-docs\nThis is not a Linux Foundation project.',
    ].join('\n\n'),
  },
  {
    id: 'non-lf-many-repos',
    description: 'Many repos of one non-LF organization in a single request',
    expectedNodes: ['non_lf_create_in_external_group'],
    repoUrls: [
      'https://github.com/anchore/binny',
      'https://github.com/anchore/chronicle',
      'https://github.com/anchore/quill',
      'https://github.com/anchore/stereoscope',
      'https://github.com/anchore/vunnel',
      'https://github.com/anchore/yardstick',
    ],
    text: [
      'Anchore OSS tools',
      [
        'Project name: Anchore OSS',
        'Not a Linux Foundation project.',
        'https://github.com/anchore/binny',
        'https://github.com/anchore/chronicle',
        'https://github.com/anchore/quill',
        'https://github.com/anchore/stereoscope',
        'https://github.com/anchore/vunnel',
        'https://github.com/anchore/yardstick',
      ].join('\n'),
    ].join('\n\n'),
  },
  {
    id: 'lf-gerrit',
    description: 'An LF project expected to exist in PCC and CDP',
    expectedNodes: [
      'lf_in_cdp_integration_none_create',
      'lf_in_cdp_github_nango_update',
      'lf_in_cdp_github_v1_human_review',
      'lf_in_pcc_not_in_cdp_human_review',
    ],
    repoUrls: ['https://github.com/gerritcodereview/gerrit'],
    text: [
      'Add Gerrit Code Review',
      'Project name: Gerrit Code Review\nRepository: https://github.com/gerritcodereview/gerrit\nThis is a Linux Foundation project.',
    ].join('\n\n'),
  },
  {
    id: 'lf-agones',
    description: 'An LF project hosted in a non-LF GitHub organization',
    expectedNodes: [
      'lf_in_cdp_integration_none_create',
      'lf_in_cdp_github_nango_update',
      'lf_in_cdp_github_v1_human_review',
      'lf_in_pcc_not_in_cdp_human_review',
    ],
    repoUrls: ['https://github.com/googleforgames/agones'],
    text: [
      'Onboard Agones',
      'Project name: Agones\nRepository: https://github.com/googleforgames/agones\nAgones is a CNCF project.',
    ].join('\n\n'),
  },
  {
    id: 'lf-recent-cncf',
    description: 'A recent CNCF project that PCC or CDP may not know yet',
    expectedNodes: ['lf_not_in_pcc_flag_human', 'lf_in_pcc_not_in_cdp_human_review'],
    repoUrls: ['https://github.com/robusta-dev/holmesgpt'],
    text: [
      'HolmesGPT onboarding',
      'Project name: HolmesGPT\nRepository: https://github.com/robusta-dev/holmesgpt\nHolmesGPT is a CNCF sandbox project.',
    ].join('\n\n'),
  },
  {
    id: 'lf-mentorship',
    description: 'An LF mentorship repository that is not a PCC project',
    expectedNodes: ['lf_not_in_pcc_flag_human', 'ambiguous_human_review'],
    repoUrls: ['https://github.com/lf-decentralized-trust-mentorships/gitmesh'],
    text: [
      'GitMesh mentorship project',
      'Project name: GitMesh\nRepository: https://github.com/lf-decentralized-trust-mentorships/gitmesh\nPart of the LF Decentralized Trust mentorship program.',
    ].join('\n\n'),
  },
  {
    id: 'lf-openssf',
    description: 'An OpenSSF project',
    expectedNodes: [
      'lf_in_cdp_integration_none_create',
      'lf_in_cdp_github_nango_update',
      'lf_in_cdp_github_v1_human_review',
      'lf_in_pcc_not_in_cdp_human_review',
    ],
    repoUrls: ['https://github.com/privateerproj/privateer-sdk'],
    text: [
      'Privateer SDK',
      'Project name: Privateer\nRepository: https://github.com/privateerproj/privateer-sdk\nThis is an OpenSSF project.',
    ].join('\n\n'),
  },
  {
    id: 'declared-non-lf-matches-pcc',
    description: 'The requester says non-LF but the name matches a PCC project',
    expectedNodes: ['ambiguous_human_review', 'non_lf_create_in_external_group'],
    repoUrls: ['https://github.com/nvidia/kai-scheduler'],
    text: [
      'KAI Scheduler',
      'Project name: KAI Scheduler\nRepository: https://github.com/nvidia/kai-scheduler\nThis is not a Linux Foundation project.',
    ].join('\n\n'),
  },
  {
    id: 'asks-about-hierarchy',
    description: 'The requester asks where the project sits in the hierarchy',
    expectedNodes: ['ambiguous_human_review'],
    repoUrls: ['https://github.com/truefoundry/kubeelasti'],
    text: [
      'Kubeelasti hierarchy question',
      'Project name: KubeElasti\nRepository: https://github.com/truefoundry/kubeelasti\nShould this be a subproject of an existing project, or a project of its own? Which parent group should it go under?',
    ].join('\n\n'),
  },
  {
    id: 'non-github-source',
    description: 'Only a non-GitHub repository',
    expectedNodes: ['not_github_source'],
    repoUrls: [],
    text: [
      'Onboard Example Tool',
      'Project name: Example Tool\nRepository: https://gitlab.com/example-org/example-tool\nNot a Linux Foundation project.',
    ].join('\n\n'),
  },
  {
    id: 'weak-name-match',
    description: 'A misspelled LF project name, to probe the weak threshold',
    expectedNodes: ['ambiguous_human_review', 'lf_in_cdp_github_nango_update'],
    repoUrls: ['https://github.com/gerritcodereview/gerrit'],
    text: [
      'Gerrit onboarding',
      'Project name: Gerrit Code Reveiw\nRepository: https://github.com/gerritcodereview/gerrit\nThis is a Linux Foundation project.',
    ].join('\n\n'),
  },
  {
    id: 'link-only',
    description: 'No repository in the text, only a link to follow',
    expectedNodes: ['ambiguous_human_review'],
    repoUrls: [],
    text: 'Please onboard my project. All the details are in https://docs.google.com/document/d/example',
  },
]
