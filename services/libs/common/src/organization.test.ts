import { describe, expect, it } from 'vitest'

import { generateOrganizationNameVariants } from './organization'

function variantsOf(name: string): string[] {
  return generateOrganizationNameVariants(name).sort()
}

describe('generateOrganizationNameVariants', () => {
  it('returns nothing for a blank name', () => {
    expect(generateOrganizationNameVariants('   ')).toEqual([])
  })

  it('lowercases the name and collapses its spaces', () => {
    expect(generateOrganizationNameVariants('  Open   MPI ')).toContain('open mpi')
  })

  it('adds the project, foundation and initiative forms of a plain name', () => {
    expect(variantsOf('CHAOSS')).toEqual([
      'chaoss',
      'chaoss foundation',
      'chaoss initiative',
      'chaoss project',
      'chaoss-foundation',
      'chaoss-initiative',
      'chaoss-project',
    ])
  })

  it('adds no suffix form to a name shorter than four characters', () => {
    expect(variantsOf('Git')).toEqual(['git'])
  })

  it('adds the suffix forms to a name of four characters', () => {
    expect(generateOrganizationNameVariants('Curl')).toContain('curl project')
  })

  it('matches a foundation to the organization named without the suffix', () => {
    const variants = generateOrganizationNameVariants('Hashgraph Foundation')

    expect(variants).toContain('hashgraph foundation')
    expect(variants).toContain('hashgraph')
  })

  it.each([
    ['AI Engineer Foundation', 'ai engineer'],
    ['Galaxy Project', 'galaxy'],
    ['Inclusive Naming Initiative', 'inclusive naming'],
  ])('drops the suffix of "%s"', (name, withoutSuffix) => {
    expect(generateOrganizationNameVariants(name)).toContain(withoutSuffix)
  })

  it('keeps the suffix when what is left is shorter than six characters', () => {
    expect(generateOrganizationNameVariants('Hydra Project')).not.toContain('hydra')
  })

  it('drops a trailing parenthesis when at least eight characters are left', () => {
    const variants = generateOrganizationNameVariants('Open Networking Foundation (ONF)')

    expect(variants).toContain('open networking foundation (onf)')
    expect(variants).toContain('open networking foundation')
    expect(variants).toContain('open networking')
    expect(generateOrganizationNameVariants('Chromium (web)')).toContain('chromium')
  })

  it('keeps a trailing parenthesis when fewer than eight characters are left', () => {
    expect(variantsOf('CUE (lang)')).toEqual(['cue (lang)', 'cue-(lang)'])
    expect(generateOrganizationNameVariants('Mozilla (MoFo)')).not.toContain('mozilla')
  })

  it('keeps a trailing parenthesis that is not balanced', () => {
    expect(generateOrganizationNameVariants('Some Name (a)b)')).not.toContain('some name')
  })

  it('adds no suffix form to a name that still holds a parenthesis', () => {
    const variants = generateOrganizationNameVariants('Open Networking Foundation (ONF)')

    expect(variants).not.toContain('open networking foundation (onf) project')
    expect(variants).not.toContain('open networking foundation (onf) foundation')
    expect(variants).not.toContain('open networking foundation (onf) initiative')
  })

  it('drops a leading "the" when at least eight characters are left', () => {
    const variants = generateOrganizationNameVariants('The Linux Foundation')

    expect(variants).toContain('the linux foundation')
    expect(variants).toContain('linux foundation')
    expect(generateOrganizationNameVariants('The Chromium')).toContain('chromium')
  })

  it('keeps a leading "the" when fewer than eight characters are left', () => {
    expect(generateOrganizationNameVariants('The Mozilla')).not.toContain('mozilla')
  })

  it('drops the article, the parenthesis and the suffix of the same name', () => {
    expect(generateOrganizationNameVariants('The Open Networking Foundation (ONF)')).toContain(
      'open networking',
    )
  })

  it('swaps hyphens and spaces in both directions', () => {
    expect(generateOrganizationNameVariants('Alpha-Omega')).toContain('alpha omega')
    expect(generateOrganizationNameVariants('Common Crawl')).toContain('common-crawl')
  })

  it('adds the name without its dots', () => {
    expect(generateOrganizationNameVariants('prompts.chat')).toContain('promptschat')
  })
})
