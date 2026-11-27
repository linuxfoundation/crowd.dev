import { z } from 'zod'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { validateOrThrow } from '@/utils/validation'
import { BadRequestError } from '@crowd/common'
import { listActiveSubprojectsForMapping } from '@crowd/data-access-layer/src/segments'
import {
  confirmSessionizeEventMappings,
  listSessionizeEventsForReview,
} from '@crowd/data-access-layer/src/sessionize'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'
import { eventReferenceSchema } from './eventSchema'
import { suggestedCatalogueEvent } from './reviewRows'

const MAX_ITEMS = 200

const bodySchema = z.object({
  items: z
    .array(
      eventReferenceSchema.extend({
        segmentId: z
          .string()
          .uuid()
          .transform((segmentId) => segmentId.toLowerCase()),
      }),
    )
    .min(1)
    .max(MAX_ITEMS)
    .refine((items) => new Set(items.map(({ eventKey }) => eventKey)).size === items.length, {
      message: 'Each eventKey may appear only once',
    }),
})

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.eventMappingEdit)

  const { items } = validateOrThrow(bodySchema, req.body)
  const reviewedBy: string = req.currentUser.id

  await SequelizeRepository.getQueryExecutor(req).tx(async (tx) => {
    const events = new Map(
      (await listSessionizeEventsForReview(tx)).map((event) => [event.eventKey, event]),
    )
    const subprojects = await listActiveSubprojectsForMapping(tx)
    const subprojectIds = new Set(subprojects.map(({ id }) => id))

    const unknownSegment = items.find(({ segmentId }) => !subprojectIds.has(segmentId))
    if (unknownSegment) {
      throw new BadRequestError(`Segment ${unknownSegment.segmentId} is not an active subproject`)
    }

    await confirmSessionizeEventMappings(
      tx,
      items.map(({ eventKey, segmentId }) => ({
        eventKey,
        segmentId,
        ...suggestedCatalogueEvent(events.get(eventKey), subprojects, segmentId),
        reviewedBy,
      })),
    )
  })

  await req.responseHandler.success(req, res, { confirmed: items.length })
}
