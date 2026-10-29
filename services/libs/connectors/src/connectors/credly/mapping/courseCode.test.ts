import { describe, expect, it } from 'vitest'

import { extractCourseCode, skuBase } from './courseCode'

describe('extractCourseCode', () => {
  it.each([
    ['LFS158: Introduction to Kubernetes', 'LFS158', 'LFS158'],
    ['CKA: Certified Kubernetes Administrator', 'CKA', 'CKA'],
    ['LFD459-JP: Kubernetes for App Developers', 'LFD459-JP', 'LFD459'],
    ['TSCB - Technical Services Certification B', 'TSCB', 'TSCB'],
    ['Cloud Cost Monitoring for FinOps with OpenCost (LFS248)', 'LFS248', 'LFS248'],
    ['  LFC131: Green Software for Practitioners  ', 'LFC131', 'LFC131'],
    ['Certified Kubernetes Administrator Beta Exam (CKA-BETA)', 'CKA-BETA', 'CKA'],
  ])('reads the code out of %j', (name, code, base) => {
    expect(extractCourseCode(name)).toMatchObject({ code, base })
  })

  it('returns the name without the code, for the title rule', () => {
    expect(extractCourseCode('LFS158: Introduction to Kubernetes')?.title).toBe(
      'Introduction to Kubernetes',
    )
    expect(extractCourseCode('TSCB - Technical Services Certification B')?.title).toBe(
      'Technical Services Certification B',
    )
    expect(extractCourseCode('Cloud Cost Monitoring (LFS248)')?.title).toBe('Cloud Cost Monitoring')
  })

  it.each([
    'Exam Developer: Certified Kubernetes Administrator',
    'LF Live Mentorship Series: Mentor',
    'Course Developer',
    'FinOps Certified Practitioner',
    'KubeCon + CloudNativeCon North America 2025 Speaker',
    'LFS201Esp: Fundamentos de Administración de Sistemas Linux',
    'cdCon 2021 Speaker',
    '5G Super Blueprint Participant',
    'ODPI Collection Logo',
    'Zero to Merge (cohort 3)',
  ])('leaves %j without a code', (name) => {
    expect(extractCourseCode(name)).toBeNull()
  })

  it('cuts a variant down to its base code at the first dash or underscore', () => {
    expect(skuBase('CKA-CN')).toBe('CKA')
    expect(skuBase('LFD459_20211011_PART_VIRT_QA')).toBe('LFD459')
    expect(skuBase('FINOPS-TOKENOMICS')).toBe('FINOPS')
    expect(skuBase(' cka2 ')).toBe('CKA2')
  })
})
