// Copyright (c) 2025 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

declare global {
  interface Window {
    transcend?: { showConsentManager?: () => void };
  }
}

export const openCookieSettings = () => {
  window.transcend?.showConsentManager?.();
};
