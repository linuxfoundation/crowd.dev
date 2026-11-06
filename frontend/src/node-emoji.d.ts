// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

declare module 'node-emoji' {
  const emoji: {
    emojify(str: string, onMissing?: (name: string) => string): string;
  };
  export default emoji;
}
