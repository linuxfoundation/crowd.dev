import { LoggerBase } from '@crowd/logging'
import {
  SlackChannel,
  SlackMessageSection,
  SlackPersona,
  sendSlackNotification,
} from '@crowd/slack'

import { IServiceOptions } from '../services/IServiceOptions'

// Slack section text limit is 3000 chars. Keep conservative to account for title + formatting.
const SLACK_SECTION_TEXT_LIMIT = 2800

/* eslint-disable class-methods-use-this */
export default class ApiResponseHandler extends LoggerBase {
  public constructor(options: IServiceOptions) {
    super(options.log)
  }

  async download(req, res, path) {
    res.download(path)
  }

  async success(_req, res, payload, status = 200) {
    if (payload !== undefined) {
      // We might want to send a custom status, even the operation succeeded
      res.status(status).send(payload)
    } else {
      res.sendStatus(200)
    }
  }

  private truncateForSlack(text: string, maxLength: number = SLACK_SECTION_TEXT_LIMIT): string {
    if (text.length <= maxLength) {
      return text
    }
    return `${text.substring(0, maxLength - 3)}...`
  }

  private sendServerErrorToSlack(
    req,
    error,
    code: number,
    options?: { sql?: string; dbErrorMessage?: string },
  ): void {
    // NOTE: Temporarily ignore specific query timeout errors for these endpoints
    // Do not remove this until https://linuxfoundation.atlassian.net/browse/CM-872, and https://linuxfoundation.atlassian.net/browse/CM-822 are completed or we have a better solution.
    const ignoredEndpoints = ['/member/autocomplete', '/member/query']

    if (
      req.method === 'POST' &&
      code === 500 &&
      ignoredEndpoints.includes(req.url) &&
      error?.name === 'SequelizeDatabaseError' &&
      error?.message?.includes('Query read timeout')
    ) {
      return
    }

    const sections: SlackMessageSection[] = []

    // Request info section
    sections.push({
      title: 'Request',
      text: `*Method:* \`${req.method}\`\n*URL:* \`${req.url}\`\n*Status Code:* \`${code}\``,
    })

    // Error info section
    const errorName = error?.name || 'Unknown'
    const errorMessage = this.truncateForSlack(error?.message || 'No message', 2000)
    sections.push({
      title: 'Error',
      text: `*Name:* \`${errorName}\`\n*Message:* ${errorMessage}`,
    })

    // SQL query section (for Sequelize errors)
    if (options?.sql) {
      const truncatedSql = this.truncateForSlack(options.sql, 2700)
      sections.push({
        title: 'SQL Query',
        text: `\`\`\`${truncatedSql}\`\`\``,
      })
    }

    // DB error message (for Sequelize errors)
    if (options?.dbErrorMessage) {
      sections.push({
        title: 'Database Error',
        text: this.truncateForSlack(options.dbErrorMessage, 2700),
      })
    }

    // Request body section (if present)
    if (req.body && Object.keys(req.body).length > 0) {
      const bodyStr = JSON.stringify(req.body, null, 2)
      const truncatedBody = this.truncateForSlack(bodyStr, 2700)
      sections.push({
        title: 'Request Body',
        text: `\`\`\`${truncatedBody}\`\`\``,
      })
    }

    // Query params section (if present)
    if (req.query && Object.keys(req.query).length > 0) {
      const queryStr = JSON.stringify(req.query, null, 2)
      const truncatedQuery = this.truncateForSlack(queryStr, 2700)
      sections.push({
        title: 'Query Params',
        text: `\`\`\`${truncatedQuery}\`\`\``,
      })
    }

    // Stack trace section
    if (error?.stack) {
      const truncatedStack = this.truncateForSlack(error.stack, 2700)
      sections.push({
        title: 'Stack Trace',
        text: `\`\`\`${truncatedStack}\`\`\``,
      })
    }

    sendSlackNotification(
      SlackChannel.CDP_ALERTS,
      SlackPersona.ERROR_REPORTER,
      `API Error ${code}: ${req.method} ${req.url}`,
      sections,
    )
  }

  async error(req, res, error) {
    if (error && error.name && error.name.includes('Sequelize')) {
      req.log.error(
        error,
        {
          code: 500,
          url: req.url,
          method: req.method,
          query: error.sql,
          body: req.body,
          errorMessage: error.original?.message,
        },
        'Database error while processing REST API request!',
      )
      this.sendServerErrorToSlack(req, error, 500, {
        sql: error.sql,
        dbErrorMessage: error.original?.message,
      })
      res.status(500).send('Internal Server Error')
    } else if (error && [400, 401, 403, 404].includes(error.code)) {
      req.log.error(
        error,
        { code: error.code, url: req.url, method: req.method, query: req.query, body: req.body },
        'Client error while processing REST API request!',
      )
      res.status(error.code).send(error.message)
    } else if (error && error.message === 'stream is not readable') {
      res.status(400).send('Request interrupted')
    } else {
      if (error.code && (!Number.isInteger(error.code) || error.code < 100 || error.code > 599)) {
        error.code = 500
      } else if (!error.code) {
        error.code = 500
      }

      req.log.error(
        error,
        { code: error.code, url: req.url, method: req.method, query: req.query, body: req.body },
        'Error while processing REST API request!',
      )

      // Send Slack notification for server errors (500-599)
      if (error.code >= 500 && error.code <= 599) {
        this.sendServerErrorToSlack(req, error, error.code)
      }

      res.status(error.code).send(error.message)
    }
  }
}
