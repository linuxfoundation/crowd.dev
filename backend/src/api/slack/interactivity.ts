import { verifySlackSignature } from './verifySignature'

export default async (req, res) => {
  if (!verifySlackSignature(req)) {
    req.log.warn('Received unverified Slack interactivity payload!')
    res.sendStatus(200)
    return
  }

  const payload = JSON.parse(req.body.payload)
  req.log.info(
    { type: payload.type, actionIds: payload.actions?.map((a) => a.action_id) },
    'Received Slack interactivity payload.',
  )

  // No interactive handlers wired up yet (CM-1791, CM-1805) - just ack.
  res.sendStatus(200)
}
