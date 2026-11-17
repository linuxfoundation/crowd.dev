import assert from 'node:assert/strict'
import { test } from 'node:test'

import { servicesForPaths } from './services.ts'
import type { Rule } from './services.ts'

const rules: Rule[] = [
  { prefix: 'services/apps/data_sink_worker/', services: ['data-sink-worker'] },
  { prefix: 'services/libs/', services: '*' },
  { prefix: 'backend/', services: ['backend'] },
]

test('maps app dir to its service', () =>
  assert.deepEqual(servicesForPaths(['services/apps/data_sink_worker/src/a.ts'], rules), [
    'data-sink-worker',
  ]))
test('shared lib makes it unknown', () =>
  assert.equal(servicesForPaths(['backend/x.ts', 'services/libs/common/y.ts'], rules), null))
test('docs only maps to nothing', () =>
  assert.deepEqual(servicesForPaths(['docs/a.md'], rules), []))
