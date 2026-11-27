import { z } from 'zod'

import SequelizeRepository from '@/database/repositories/sequelizeRepository'
import { validateOrThrow } from '@/utils/validation'
import { searchActiveSubprojectsForMapping } from '@crowd/data-access-layer/src/segments'

import Permissions from '../../security/permissions'
import PermissionChecker from '../../services/user/permissionChecker'

const querySchema = z.object({
  search: z.string().max(100).default(''),
  limit: z.coerce.number().int().min(1).max(100).default(50),
  offset: z.coerce.number().int().min(0).default(0),
})

export default async (req, res) => {
  new PermissionChecker(req).validateHas(Permissions.values.eventMappingRead)

  const { search, limit, offset } = validateOrThrow(querySchema, req.query)

  const { rows, total } = await searchActiveSubprojectsForMapping(
    SequelizeRepository.getQueryExecutor(req),
    { search: search.trim(), limit, offset },
  )

  await req.responseHandler.success(req, res, {
    rows: rows.map(({ id, name, groupName }) => ({ segmentId: id, name, groupName })),
    total,
  })
}
