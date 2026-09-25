import { describe, expect, it } from 'vitest'

import { readErrorBody } from './onboarder'

describe('readErrorBody', () => {
  it('returns the response body text', async () => {
    const response = new Response('{"error":"insightsProjects slug already exists"}')

    expect(await readErrorBody(response)).toBe('{"error":"insightsProjects slug already exists"}')
  })

  it('truncates a body longer than 500 characters', async () => {
    const response = new Response('a'.repeat(600))

    const body = await readErrorBody(response)

    expect(body).toBe(`${'a'.repeat(500)}…`)
  })

  it('returns an empty string when the body cannot be read', async () => {
    const response = new Response(null)
    // Consuming the body once locks the stream, so a second read fails.
    await response.text()

    expect(await readErrorBody(response)).toBe('')
  })
})
