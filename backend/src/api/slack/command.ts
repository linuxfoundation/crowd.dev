import { SLACK_CONFIG } from '../../conf/index'
import SlackCommandService from '../../services/slackCommandService'

export default async (req, res) => {
  // verify request
  if (
    req.body.token === SLACK_CONFIG.appToken &&
    req.body.team_id === SLACK_CONFIG.teamId &&
    req.body.api_app_id === SLACK_CONFIG.appId
  ) {
    const command = req.body.command
    const params = req.body.text
    const username = req.body.user_name
    const userId = req.body.user_id
    const responseUrl = req.body.response_url

    try {
      const result = await new SlackCommandService(req).processCommand(
        command,
        params,
        username,
        userId,
        responseUrl,
      )
      res.setHeader('content-type', 'application/json')
      res.send(result)
    } catch (err) {
      req.log.error(err, 'Error processing slack command!')
      res.setHeader('content-type', 'application/json')
      const blocks = [
        {
          type: 'section',
          text: {
            type: 'mrkdwn',
            text: '*Error processing slack command!*',
          },
        },
        {
          type: 'section',
          text: {
            type: 'mrkdwn',
            text: `\`\`\`${JSON.stringify(err, undefined, 2).substring(0, 2500)}\`\`\``,
          },
        },
      ]

      res.send({
        blocks,
      })
    }
  } else {
    req.log.warn({ data: JSON.stringify(req.body) }, 'Received unverified slack command!')
    res.sendStatus(200)
  }
}
