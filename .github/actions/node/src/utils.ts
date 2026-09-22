import fs from 'fs'

import dotenv from 'dotenv'

import { IBuilderDefinition } from './types'

let definitions: IBuilderDefinition[] | undefined

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
      let services: string[] = []
      if (parsed.SERVICES !== undefined) {
        services = parsed.SERVICES.split(' ')
          .map((s) => s.trim())
          .filter((s) => s.length > 0)
      }

      let prioritizedServices: string[] = []
      if (parsed.PRIORITIZED !== undefined) {
        prioritizedServices = parsed.PRIORITIZED.split(' ')
          .map((s) => s.trim())
          .filter((s) => s.length > 0)
      }

      results.push({
        imageName,
        dockerRepository,
        services,
        prioritizedServices,
      })
    }
  }

  definitions = results

  return results
}
