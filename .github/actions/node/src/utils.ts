import fs from 'fs'

import dotenv from 'dotenv'

import { IBuilderDefinition } from './types'

let definitions: IBuilderDefinition[] | undefined

const parseList = (value: string | undefined): string[] =>
  (value ?? '')
    .split(' ')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)

export const getBuilderDefinitions = async (): Promise<IBuilderDefinition[]> => {
  if (definitions !== undefined) {
    return definitions
  }

  const results: IBuilderDefinition[] = []
  const files = fs.readdirSync('./scripts/builders')

  for (const result of files) {
    if (result.endsWith('.env')) {
      const content = fs.readFileSync(`./scripts/builders/${result}`, 'utf-8')
      const parsed = dotenv.parse(content)

      const imageName = result.split('.env')[0]
      const dockerRepository = parsed.REPO

      results.push({
        imageName,
        dockerRepository,
        services: parseList(parsed.SERVICES),
        prioritizedServices: parseList(parsed.PRIORITIZED),
        cronJobs: parseList(parsed.CRONJOBS),
      })
    }
  }

  definitions = results

  return results
}
