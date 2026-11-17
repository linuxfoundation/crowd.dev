import assert from 'node:assert/strict'
import { test } from 'node:test'

import { parseReleaseServices, renderReleaseBody } from './release-body.ts'

test('round-trips services', () =>
  assert.deepEqual(
    parseReleaseServices(
      renderReleaseBody({ title: 'feat: x', number: 5, author: 'a', services: ['api'] }),
    ),
    ['api'],
  ))

test('unknown services render as * and parse to null', () => {
  const body = renderReleaseBody({ title: 'feat: x', number: null, author: null, services: null })
  assert.match(body, /<!-- release-services: "\*" -->$/)
  assert.equal(parseReleaseServices(body), null)
})

test('full body layout', () =>
  assert.equal(
    renderReleaseBody({ title: 'feat: x', number: 5, author: 'a', services: ['api', 'web'] }),
    'feat: x (#5) — @a\nServices: api, web\n<!-- release-services: ["api","web"] -->',
  ))

test('null number and author are left out', () =>
  assert.equal(
    renderReleaseBody({ title: 'fix: y', number: null, author: null, services: null }).split(
      '\n',
    )[0],
    'fix: y',
  ))

test('author only and number only', () => {
  const first = (number: number | null, author: string | null) =>
    renderReleaseBody({ title: 't', number, author, services: [] }).split('\n')[0]
  assert.equal(first(3, null), 't (#3)')
  assert.equal(first(null, 'bob'), 't — @bob')
})

test('unknown services get the shared code line, empty services omit the line', () => {
  const unknown = renderReleaseBody({ title: 't', number: 1, author: 'a', services: null })
  assert.match(unknown, /\nServices: all \(shared code\)\n/)
  const none = renderReleaseBody({ title: 't', number: 1, author: 'a', services: [] })
  assert.doesNotMatch(none, /Services:/)
  assert.deepEqual(parseReleaseServices(none), [])
})

test('missing or malformed marker parses to null', () => {
  assert.equal(parseReleaseServices('no marker here'), null)
  assert.equal(parseReleaseServices('<!-- release-services: nope -->'), null)
})
