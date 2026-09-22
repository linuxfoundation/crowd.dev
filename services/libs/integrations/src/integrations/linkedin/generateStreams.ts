import { GenerateStreamsHandler } from '../../types'
import { ILinkedInOrganization } from './api/types'
import { ILinkedInRootOrganizationStream, LinkedinStreamType } from './types'

const handler: GenerateStreamsHandler = async (ctx) => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const organization: ILinkedInOrganization = (ctx.integration.settings as any).organizations.find(
    (o) => o.inUse === true,
  )

  if (!organization) {
    await ctx.abortRunWithError('No organization configured!')
    return
  }

  await ctx.publishStream<ILinkedInRootOrganizationStream>(
    `${LinkedinStreamType.ORGANIZATION}-${organization.name}`,
    {
      organization: organization.name,
      organizationUrn: organization.organizationUrn,
    },
  )
}

export default handler
