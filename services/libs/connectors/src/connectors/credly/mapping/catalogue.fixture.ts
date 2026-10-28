import type { Catalogue } from './catalogue'

// Rows copied on 2026-10-27 from ANALYTICS.bronze_census_ti.courses, silver_dim._ti_course_groups_to_projects
// and silver_fact.enrollments, limited to the cases the mapping review calls out.
const K8S_GROUP = '60ba5d16-955b-4e2d-b84a-bab6b0f8560a'

const course = (sku: string, title: string, courseGroupId: string, archived = false) => ({
  sku,
  title,
  courseGroupId,
  isTestOrArchived: archived,
})

export const catalogueFixture: Catalogue = {
  courses: [
    course('FINOPS-AI-INTRO', 'AI Value: Introduction', '63639010-21e9-46ab-8df3-2639d9dcde7d'),
    course('FINOPS-AI-LEVEL1', 'AI Value Trained: Level 1', '25423933-dd98-4284-bbf5-56e7f37e0845'),
    course('FINOPS-AI-LEVEL2', 'AI Value Trained: Level 2', 'feb6fef2-8f1f-47af-9e5f-2e6d8575e174'),
    course('FINOPS-AI-LEVEL3', 'AI Value Trained: Level 3', '261c738f-3bd4-4b3e-8334-2d11bd17a3b2'),
    course('FINOPS-AI-CERT', 'FinOps Certified: AI Value', 'e732adf9-eaa2-4d6f-bc36-2b0fee8e539d'),
    course(
      'FINOPS-TOKENOMICS',
      'AI Tokenomics Fundamentals',
      'ed0665eb-7ca4-4ad4-a855-db2e0287db43',
    ),
    course('FOCP', 'FinOps Certified Practitioner Exam', '6ecf6d70-4df7-4908-bf05-237f0ebf6e81'),
    course(
      'CKA',
      'Certified Kubernetes Administrator (CKA)',
      '415c3ca0-d535-4ddc-a650-4b11057ee85c',
    ),
    course(
      'CKA-CN',
      'Certified Kubernetes Administrator China Exam (CKA-CN)',
      '7dc58015-102c-4837-b645-2f87bae6c581',
    ),
    course(
      'CKA-JP',
      'Certified Kubernetes Administrator Japan Exam (CKA-JP)',
      '2aca47e1-d7af-48a5-aba9-f38bfef00ab0',
    ),
    course(
      'CKA-BETA',
      'Certified Kubernetes Administrator Beta Exam (CKA-BETA)',
      '33c2259e-c45b-420c-aead-01017c86385d',
    ),
    course(
      'CKA-SINGLE',
      'Certified Kubernetes Administrator Single Attempt (CKA-SINGLE)',
      '9a07ee1f-f69e-4d26-acef-9068c531fb8c',
    ),
    course(
      'CKA-LFTEST',
      'Certified Kubernetes Administrator LF TEST (CKA-LFTEST)',
      '6b04e13b-c2f0-4054-b1a8-b0a45425c8f4',
      true,
    ),
    course(
      'CKA-EVENT',
      'Certified Kubernetes Administrator Onsite Event Exam (CKA-EVENT) (Archived)',
      '3dbca605-ed75-4299-8d6a-4152354d3f6f',
      true,
    ),
    course(
      'CKA-CN-TC',
      'Certified Kubernetes Administrator China Test Center (CKA-CN-TC) (Archived)',
      '41aaa76e-e018-4c52-b152-55022a31fd59',
      true,
    ),
    course(
      'CKA2020-BETA',
      'Certified Kubernetes Administrator 2020 (CKA2020-BETA) (Archived)',
      '077bd5a8-37e3-45a2-b020-d0bbf35df8af',
      true,
    ),
    course(
      'CKA2 ',
      'Certified Kubernetes Administrator (CKA2)',
      '6db892b3-77e4-4867-a69b-aa5ecf1310fe',
    ),
    course(
      'CKAD',
      'Certified Kubernetes Application Developer (CKAD)',
      '792d8cd0-e8a9-4b26-9a83-ddb9881d8116',
    ),
    course(
      'CKS',
      'Certified Kubernetes Security Specialist (CKS)',
      '3c44a720-ac9b-444c-86a5-1eaa27cae083',
    ),
    course(
      'CKS-BETA',
      'Certified Kubernetes Security Specialist Beta (CKS-BETA)',
      '9024ab47-03b9-41e9-89a8-0ca57e4fd86d',
    ),
    course('ICA', 'Istio Certified Associate (ICA)', 'f5b36335-7066-4987-8d90-af99ee12c223'),
    course(
      'ICA-BETA',
      'Istio Certified Associate Beta Exam (ICA-BETA)',
      '60f6821c-be2b-48fa-a422-13ccf8a26626',
    ),
    course(
      'KCNA',
      'Kubernetes and Cloud Native Associate Exam (KCNA)',
      '2461f2db-419b-40d2-8e3a-01062c92f94c',
    ),
    course(
      'KCNA-SINGLE',
      'Kubernetes and Cloud Native Associate Single Attempt (KCNA-SINGLE)',
      '8efb4199-5b95-4675-9ae1-399abc8d5ac9',
    ),
    course(
      'KCNA-ANDELA',
      'Kubernetes and Cloud Native Associate Andela (KCNA-ANDELA)',
      'fe3906ca-30d2-4de5-aff3-dd8f1ef773b6',
    ),
    course('LFS158', 'Introduction to Kubernetes (LFS158)', 'e7546f58-b748-4a27-9207-8618ba4c9b2a'),
    course('LFD459', 'Kubernetes for App Developers (LFD459)', K8S_GROUP),
    course(
      'LFD459_20240325-PART_VIRT_SFEIR',
      'Kubernetes for App Developers (LFD459) MARCH 25, 2024 PARTNER (SFEIR) (Archived)',
      K8S_GROUP,
      true,
    ),
    course(
      'LFD459_20211011_PART_VIRT_QA',
      'Kubernetes for App Developers (LFD459) OCTOBER 11, 2021 PARTNER (QA) (Archived)',
      K8S_GROUP,
      true,
    ),
    course(
      'LFD459_20261103_PART_VIRT_SFEIR',
      'Kubernetes for App Developers (LFD459) NOVEMBER 3, 2026 PARTNER (SFEIR)',
      K8S_GROUP,
    ),
    course(
      'LFS201',
      'Essentials of Linux System Administration (LFS201) (Archived)',
      'b7c385fe-de2c-4bd8-8adb-0435424eeb82',
      true,
    ),
    course(
      'LFS201-JP',
      'Linuxシステム管理入門 (LFS201-JP)',
      '92838cbb-601e-40ad-8af2-daa3d5324c09',
    ),
    course(
      'LFS201-VMW',
      'Essentials of Linux System Administration (LFS201-VMW) (Archived)',
      '0c86c976-cfb2-40a9-83a8-3098681d24ba',
      true,
    ),
    course(
      'LFS201-ESP',
      'Fundamentos de Administración de Sistemas Linux (LFS201-Esp) (Archived)',
      '83698d48-fbcd-4564-8fae-f7b71292385c',
      true,
    ),
    course(
      'LFS201-PRT',
      'Elementos Básicos de Administração de Sistemas (LFS201-Prt) (Archived)',
      'a1e2b19b-f414-46ba-916b-82c2c87878ba',
      true,
    ),
    course(
      'LFS201-JP-COPY',
      'Linuxシステム管理入門 (LFS201-JP) Copy (Archived)',
      '777c9506-6803-49b2-947d-cb069c32ef80',
      true,
    ),
    course(
      'LFS272',
      'Hyperledger Fabric Administration (LFS272) (Archived)',
      '8759f93c-88db-4656-8827-570e6baf5ca1',
      true,
    ),
    course(
      'LFS272-JP',
      'Hyperledger Fabric管理 (LFS272-JP)',
      '52e30f9a-ddce-4838-9785-c24d9c71f109',
    ),
    course(
      'LFS272-VMW',
      'Hyperledger Fabric Administration (LFS272-VMW) (Archived)',
      '9d014896-4f4a-4a98-a44d-ed6e19e5cd2c',
      true,
    ),
    course(
      'LFC131',
      'Green Software for Practitioners (LFC131) (Archived)',
      '457086cc-68bf-4a5d-8791-e309c55487cf',
      true,
    ),
    course(
      'LFC101',
      'The Inclusive Speaker Orientation (LFC101)',
      'e8f0f9ba-9fb1-4b2b-b2c6-2f9bd6e70bc4',
    ),
    course(
      'LFC191',
      'Open Source Licensing Basics for Software Developers (LFC191)',
      'cf0d0085-f4e1-4fec-8b11-a0af01325ea2',
    ),
    course(
      'JSNAD',
      'OpenJS Node.js Application Developer (JSNAD) (Archived)',
      'ecffd259-cda2-4e07-a411-9f0e40d42861',
      true,
    ),
    course(
      'CFCD',
      'Cloud Foundry Certified Developer (CFCD) (Archived)',
      '1b28b820-ebd4-4941-8ee3-e86548519800',
      true,
    ),
    course('LFS114', 'Introduction to free5GC (LFS114)', '02134711-fbf7-4e65-985b-6fa1a34ed5c5'),
    course(
      'LFS118',
      'Ethical Principles for Conversational AI (LFS118)',
      '8e733536-bed5-4454-81a1-2a0de02bcbcf',
    ),
    course(
      'LFS120',
      'Conversational AI: Ensuring Compliance and Mitigating Risks (LFS120)',
      '7faebda3-d42c-4082-b110-599b69b0cdac',
    ),
    course('PTCA', 'PyTorch Certified Associate (PTCA)', '2005b112-f961-4977-984a-0d8eed0062ad'),
    course('TSCB', 'Linux Foundation TSCB (TSCB)', 'e8dab5a4-987b-4785-a3b6-d1d651563ea1'),
    course(
      'LFS248',
      'Cloud Cost Monitoring for FinOps with OpenCost (LFS248)',
      'dedef594-a880-422d-9aa6-a596ce7de042',
    ),
  ],
  courseGroups: [
    {
      courseGroupId: '2461f2db-419b-40d2-8e3a-01062c92f94c',
      title: 'Kubernetes and Cloud Native Associate Exam (KCNA)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: 'ed0665eb-7ca4-4ad4-a855-db2e0287db43',
      title: 'AI Tokenomics Fundamentals',
      projectSlug: 'tokenomics-foundation',
    },
    {
      courseGroupId: 'f5b36335-7066-4987-8d90-af99ee12c223',
      title: 'Istio Certified Associate (ICA)',
      projectSlug: 'istio',
    },
    {
      courseGroupId: '261c738f-3bd4-4b3e-8334-2d11bd17a3b2',
      title: 'AI Value Trained: Level 3',
      projectSlug: 'finops',
    },
    {
      courseGroupId: '63639010-21e9-46ab-8df3-2639d9dcde7d',
      title: 'AI Value: Introduction',
      projectSlug: 'finops',
    },
    {
      courseGroupId: '6db892b3-77e4-4867-a69b-aa5ecf1310fe',
      title: 'Certified Kubernetes Administrator (CKA2)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: '9024ab47-03b9-41e9-89a8-0ca57e4fd86d',
      title: 'Certified Kubernetes Security Specialist Beta (CKS-BETA)',
      projectSlug: 'cncf',
    },
    {
      courseGroupId: '60f6821c-be2b-48fa-a422-13ccf8a26626',
      title: 'Istio Certified Associate Beta Exam (ICA-BETA)',
      projectSlug: 'cncf',
    },
    {
      courseGroupId: '792d8cd0-e8a9-4b26-9a83-ddb9881d8116',
      title: 'Certified Kubernetes Application Developer (CKAD)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: 'e732adf9-eaa2-4d6f-bc36-2b0fee8e539d',
      title: 'FinOps Certified: AI Value',
      projectSlug: 'finops',
    },
    {
      courseGroupId: 'e8f0f9ba-9fb1-4b2b-b2c6-2f9bd6e70bc4',
      title: 'The Inclusive Speaker Orientation (LFC101)',
      projectSlug: 'tlf',
    },
    {
      courseGroupId: '8efb4199-5b95-4675-9ae1-399abc8d5ac9',
      title: 'Kubernetes and Cloud Native Associate Single Attempt (KCNA-SINGLE)',
      projectSlug: 'cncf',
    },
    {
      courseGroupId: '33c2259e-c45b-420c-aead-01017c86385d',
      title: 'Certified Kubernetes Administrator Beta Exam (CKA-BETA)',
      projectSlug: 'cncf',
    },
    {
      courseGroupId: '415c3ca0-d535-4ddc-a650-4b11057ee85c',
      title: 'Certified Kubernetes Administrator (CKA)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: '3c44a720-ac9b-444c-86a5-1eaa27cae083',
      title: 'Certified Kubernetes Security Specialist (CKS)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: '9a07ee1f-f69e-4d26-acef-9068c531fb8c',
      title: 'Certified Kubernetes Administrator Single Attempt (CKA-SINGLE)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: 'feb6fef2-8f1f-47af-9e5f-2e6d8575e174',
      title: 'AI Value Trained: Level 2',
      projectSlug: 'finops',
    },
    {
      courseGroupId: '7faebda3-d42c-4082-b110-599b69b0cdac',
      title: 'Conversational AI: Ensuring Compliance and Mitigating Risks (LFS120)',
      projectSlug: 'open-voice-network',
    },
    {
      courseGroupId: '25423933-dd98-4284-bbf5-56e7f37e0845',
      title: 'AI Value Trained: Level 1',
      projectSlug: 'finops',
    },
    {
      courseGroupId: 'fe3906ca-30d2-4de5-aff3-dd8f1ef773b6',
      title: 'Kubernetes and Cloud Native Associate (KCNA-ANDELA)',
      projectSlug: 'cncf',
    },
    {
      courseGroupId: '02134711-fbf7-4e65-985b-6fa1a34ed5c5',
      title: 'Introduction to free5GC (LFS114)',
      projectSlug: 'free5gc',
    },
    {
      courseGroupId: '8e733536-bed5-4454-81a1-2a0de02bcbcf',
      title: 'Ethical Principles for Conversational AI (LFS118)',
      projectSlug: 'open-voice-network',
    },
    {
      courseGroupId: '7dc58015-102c-4837-b645-2f87bae6c581',
      title: 'Certified Kubernetes Administrator China Exam (CKA-CN)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: '92838cbb-601e-40ad-8af2-daa3d5324c09',
      title: 'Linuxシステム管理入門 (LFS201-JP)',
      projectSlug: 'tlf',
    },
    {
      courseGroupId: K8S_GROUP,
      title: 'Kubernetes for App Developers (LFD459)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: 'e7546f58-b748-4a27-9207-8618ba4c9b2a',
      title: 'Introduction to Kubernetes (LFS158)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: '2005b112-f961-4977-984a-0d8eed0062ad',
      title: 'PyTorch Certified Associate (PTCA)',
      projectSlug: 'pytorch',
    },
    {
      courseGroupId: '6ecf6d70-4df7-4908-bf05-237f0ebf6e81',
      title: 'FinOps Certified Practitioner',
      projectSlug: 'finops',
    },
    {
      courseGroupId: 'e8dab5a4-987b-4785-a3b6-d1d651563ea1',
      title: 'Linux Foundation TSCB (TSCB)',
      projectSlug: 'tlf',
    },
    {
      courseGroupId: '52e30f9a-ddce-4838-9785-c24d9c71f109',
      title: 'Hyperledger Fabric管理 (LFS272-JP)',
      projectSlug: 'fabric',
    },
    {
      courseGroupId: 'dedef594-a880-422d-9aa6-a596ce7de042',
      title: 'Cloud Cost Monitoring for FinOps with OpenCost (LFS248)',
      projectSlug: 'opencost',
    },
    {
      courseGroupId: 'cf0d0085-f4e1-4fec-8b11-a0af01325ea2',
      title: 'Open Source Licensing Basics for Software Developers (LFC191)',
      projectSlug: 'tlf',
    },
    {
      courseGroupId: '41aaa76e-e018-4c52-b152-55022a31fd59',
      title: 'Certified Kubernetes Administrator China Test Center (CKA-CN-TC) (Archived)',
      projectSlug: 'k8s',
    },
    {
      courseGroupId: '2aca47e1-d7af-48a5-aba9-f38bfef00ab0',
      title: 'Certified Kubernetes Administrator Japan Exam (CKA-JP)',
      projectSlug: 'k8s',
    },
  ],
  enrollments: [
    { courseCode: 'CKA', projectSlug: 'k8s', enrollments: 821062 },
    { courseCode: 'CKA-BETA', projectSlug: 'cncf', enrollments: 483 },
    { courseCode: 'CKA-CN', projectSlug: 'k8s', enrollments: 98561 },
    { courseCode: 'CKA-JP', projectSlug: 'k8s', enrollments: 11208 },
    { courseCode: 'CKA-SINGLE', projectSlug: 'k8s', enrollments: 931 },
    { courseCode: 'CKAD', projectSlug: 'k8s', enrollments: 399850 },
    { courseCode: 'CKAD-BETA', projectSlug: 'k8s', enrollments: 872 },
    { courseCode: 'CKAD-CN', projectSlug: 'k8s', enrollments: 5981 },
    { courseCode: 'CKAD-JP', projectSlug: 'k8s', enrollments: 5304 },
    { courseCode: 'CKAD-SINGLE', projectSlug: 'k8s', enrollments: 236 },
    { courseCode: 'FINOPS-TOKENOMICS', projectSlug: 'tokenomics-foundation', enrollments: 5 },
    { courseCode: 'LFS158', projectSlug: 'k8s', enrollments: 190630 },
    { courseCode: 'LFS201-JP', projectSlug: 'tlf', enrollments: 1580 },
    { courseCode: 'LFS272-JP', projectSlug: 'fabric', enrollments: 396 },
  ],
}
