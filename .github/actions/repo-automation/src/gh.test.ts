import assert from 'node:assert/strict'
import { test } from 'node:test'

import { runCaptureStdout } from './gh.ts'

test('returns stdout on a non-zero exit', () =>
  assert.equal(
    runCaptureStdout(process.execPath, ['-e', "process.stdout.write('[1]');process.exit(1)"]),
    '[1]',
  ))
test('throws when the binary is missing', () =>
  assert.throws(() => runCaptureStdout('definitely-not-a-binary-xyz', [])))
