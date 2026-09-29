// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

// A scoring run that ran out of its time budget; retrying it only doubles the wait.
export class DeadlineExceededError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'DeadlineExceededError'
  }
}
