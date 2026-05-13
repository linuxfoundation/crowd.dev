import express from 'express'

import { MemberSyncService } from '@crowd/opensearch'

import { ApiRequest } from '../middleware'
import { asyncWrap } from '../middleware/error'

const router = express.Router()

const syncService = (req: ApiRequest): MemberSyncService =>
  new MemberSyncService(req.redisClient, req.pgStore, req.opensearch, req.log)

router.post(
  '/sync/members',
  asyncWrap(async (req: ApiRequest, res) => {
    const memberSyncService = syncService(req)

    const { memberId } = req.body
    try {
      req.log.trace(`Calling memberSyncService.syncMembers for ${memberId}`)
      await memberSyncService.syncMembers(memberId)
      res.sendStatus(200)
    } catch (error) {
      req.log.error(error)
      res.status(500).send(error.message)
    }
  }),
)

router.post(
  '/sync/organization/members',
  asyncWrap(async (req: ApiRequest, res) => {
    const { organizationId, lastId, batchSize, syncFrom } = req.body
    try {
      const result = await syncService(req).syncOrganizationMembers(organizationId, {
        lastId: lastId ?? undefined,
        batchSize: batchSize ?? undefined,
        syncFrom: syncFrom ? new Date(syncFrom) : null,
      })
      res.json(result)
    } catch (error) {
      req.log.error(error)
      res.status(500).send(error.message)
    }
  }),
)

router.post(
  '/cleanup/members',
  asyncWrap(async (req: ApiRequest, res) => {
    const memberSyncService = syncService(req)

    try {
      req.log.trace(`Calling memberSyncService.cleanupMemberIndex`)
      await memberSyncService.cleanupMemberIndex()
      res.sendStatus(200)
    } catch (error) {
      req.log.error(error)
      res.status(500).send(error.message)
    }
  }),
)

router.post(
  '/cleanup/member',
  asyncWrap(async (req: ApiRequest, res) => {
    const memberSyncService = syncService(req)

    const { memberId } = req.body
    try {
      req.log.trace(`Calling memberSyncService.removeMember for ${memberId}`)
      await memberSyncService.removeMember(memberId)
      res.sendStatus(200)
    } catch (error) {
      req.log.error(error)
      res.status(500).send(error.message)
    }
  }),
)

export default router
