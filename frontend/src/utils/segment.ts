// Copyright (c) 2025 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

const LFX_SEGMENT_URL =
  'https://lfx-segment.platform.linuxfoundation.org/latest/lfx-segment-analytics.min.js';

type EventProperties = Record<string, unknown>;

interface LfxSegmentsAnalytics {
  init: (writeKey?: string) => Promise<void>;
  track: (event: string, properties?: EventProperties) => Promise<void>;
  page: (name?: string, properties?: EventProperties) => Promise<void>;
  identify: (userId: string | null, traits?: EventProperties) => Promise<void>;
}

declare global {
  interface Window {
    LfxAnalytics?: {
      LfxSegmentsAnalytics?: { getInstance: () => LfxSegmentsAnalytics };
    };
  }
}

let lfxAnalytics: Promise<LfxSegmentsAnalytics | null> = Promise.resolve(null);

const injectScript = (src: string): Promise<void> =>
  new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = src;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`Failed to load ${src}`));
    document.head.appendChild(script);
  });

const loadLfxAnalytics = async (writeKey: string): Promise<LfxSegmentsAnalytics | null> => {
  await injectScript(LFX_SEGMENT_URL);
  const instance = window.LfxAnalytics?.LfxSegmentsAnalytics?.getInstance() ?? null;
  // init() must be called before any queued call reaches the instance, or that call is dropped as held.
  instance
    ?.init(writeKey)
    .catch((error: unknown) => console.error('Segment failed to load', error));
  return instance;
};

export const loadSegment = (writeKey: string): void => {
  lfxAnalytics = loadLfxAnalytics(writeKey).catch(() => null);
};

// No call site awaits or catches these calls, so a failure is logged here instead of becoming an unhandled rejection.
const withAnalytics = (send: (instance: LfxSegmentsAnalytics) => Promise<void>): Promise<void> =>
  lfxAnalytics
    .then((instance) => (instance ? send(instance) : undefined))
    .catch((error: unknown) => console.error('Segment call failed', error));

export const analytics = {
  track: (event: string, properties?: EventProperties) =>
    withAnalytics((instance) => instance.track(event, properties)),
  page: (name: string) => withAnalytics((instance) => instance.page(name)),
  identify: (userId: string, traits: EventProperties) =>
    withAnalytics((instance) => instance.identify(userId, traits)),
};

export const isRealSegmentKey = (key?: string): boolean => {
  if (!key) {
    return false;
  }

  return !key.startsWith('CROWD_VUE_APP') && !key.startsWith('%VUE_APP');
};
