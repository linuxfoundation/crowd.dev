import { beforeEach, describe, expect, it, vi } from 'vitest'

const postMessage = vi.fn()
const update = vi.fn()
const getPermalink = vi.fn()

vi.mock('@slack/web-api', () => ({
  WebClient: vi.fn().mockImplementation(function WebClient() {
    return { chat: { postMessage, update, getPermalink } }
  }),
}))

const getSlackBotConfig = vi.fn()
vi.mock('./botConfig', () => ({ getSlackBotConfig }))

describe('chatClient', () => {
  beforeEach(() => {
    vi.resetModules()
    postMessage.mockReset()
    update.mockReset()
    getPermalink.mockReset()
    getSlackBotConfig.mockReset()
  })

  describe('postSlackMessage', () => {
    it('returns an error when the bot token is not configured', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: undefined })
      const { postSlackMessage } = await import('./chatClient.js')

      const result = await postSlackMessage({ channel: '#general', text: 'hi' })

      expect(result).toEqual({ ok: false, error: 'Slack bot client not available' })
      expect(postMessage).not.toHaveBeenCalled()
    })

    it('returns the message timestamp on success', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: 'xoxb-test' })
      postMessage.mockResolvedValue({ ts: '123.456' })
      const { postSlackMessage } = await import('./chatClient.js')

      const result = await postSlackMessage({ channel: '#general', text: 'hi' })

      expect(result).toEqual({ ok: true, ts: '123.456' })
    })

    it('returns the error message when the SDK call is rejected', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: 'xoxb-test' })
      postMessage.mockRejectedValue(new Error('channel_not_found'))
      const { postSlackMessage } = await import('./chatClient.js')

      const result = await postSlackMessage({ channel: '#general', text: 'hi' })

      expect(result).toEqual({ ok: false, error: 'channel_not_found' })
    })
  })

  describe('updateSlackMessage', () => {
    it('returns an error when the bot token is not configured', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: undefined })
      const { updateSlackMessage } = await import('./chatClient.js')

      const result = await updateSlackMessage({ channel: '#general', ts: '123.456', text: 'hi' })

      expect(result).toEqual({ ok: false, error: 'Slack bot client not available' })
      expect(update).not.toHaveBeenCalled()
    })

    it('returns ok on success', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: 'xoxb-test' })
      update.mockResolvedValue({})
      const { updateSlackMessage } = await import('./chatClient.js')

      const result = await updateSlackMessage({ channel: '#general', ts: '123.456', text: 'hi' })

      expect(result).toEqual({ ok: true })
    })

    it('returns the error message when the SDK call is rejected', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: 'xoxb-test' })
      update.mockRejectedValue(new Error('message_not_found'))
      const { updateSlackMessage } = await import('./chatClient.js')

      const result = await updateSlackMessage({ channel: '#general', ts: '123.456', text: 'hi' })

      expect(result).toEqual({ ok: false, error: 'message_not_found' })
    })
  })

  describe('getSlackPermalink', () => {
    it('returns null when the bot token is not configured', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: undefined })
      const { getSlackPermalink } = await import('./chatClient.js')

      expect(await getSlackPermalink('C1', '123.456')).toBeNull()
      expect(getPermalink).not.toHaveBeenCalled()
    })

    it('returns the permalink on success', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: 'xoxb-test' })
      getPermalink.mockResolvedValue({ permalink: 'https://slack.test/archives/C1/p123456' })
      const { getSlackPermalink } = await import('./chatClient.js')

      expect(await getSlackPermalink('C1', '123.456')).toBe(
        'https://slack.test/archives/C1/p123456',
      )
      expect(getPermalink).toHaveBeenCalledWith({ channel: 'C1', message_ts: '123.456' })
    })

    it('returns null when the response has no permalink', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: 'xoxb-test' })
      getPermalink.mockResolvedValue({})
      const { getSlackPermalink } = await import('./chatClient.js')

      expect(await getSlackPermalink('C1', '123.456')).toBeNull()
    })

    it('returns null when the SDK call is rejected', async () => {
      getSlackBotConfig.mockReturnValue({ botToken: 'xoxb-test' })
      getPermalink.mockRejectedValue(new Error('message_not_found'))
      const { getSlackPermalink } = await import('./chatClient.js')

      expect(await getSlackPermalink('C1', '123.456')).toBeNull()
    })
  })
})
