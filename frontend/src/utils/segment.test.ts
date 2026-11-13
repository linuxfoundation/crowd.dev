// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { readFileSync } from 'node:fs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

interface FakeScript {
  src?: string;
  async?: boolean;
  onload?: () => void;
  onerror?: () => void;
}

const LFX_LATEST_URL =
  'https://lfx-segment.platform.linuxfoundation.org/latest/lfx-segment-analytics.min.js';
const AIRGAP_URL = 'https://transcend-cdn.com/cm/f484e2d0-ad2e-43a9-9d64-d07f6fa20966/airgap.js';

const injected: FakeScript[] = [];
const calls: string[] = [];

const record = (method: string, ...args: unknown[]) => {
  const parts = args
    .filter((arg) => arg !== undefined)
    .map((arg) => (typeof arg === 'string' ? arg : JSON.stringify(arg)));
  calls.push([method, ...parts].join(' '));
};

const fakeLibrary = {
  init: vi.fn(async (writeKey?: string) => record('init', writeKey)),
  track: vi.fn(async (event: string, properties?: Record<string, unknown>) =>
    record('track', event, properties),
  ),
  page: async (name?: string) => record('page', name),
  identify: async (userId: string | null, traits?: Record<string, unknown>) =>
    record('identify', userId, traits),
};

// loadSegment keeps module-level state, so each test imports a fresh copy after vi.resetModules().
const freshSegment = () => import('./segment');

const libraryLoads = () => {
  window.LfxAnalytics = { LfxSegmentsAnalytics: { getInstance: () => fakeLibrary } };
  injected[0].onload?.();
};

beforeEach(() => {
  injected.length = 0;
  calls.length = 0;
  fakeLibrary.init.mockClear();
  fakeLibrary.track.mockClear();
  vi.resetModules();
  vi.stubGlobal('window', {});
  vi.stubGlobal('document', {
    createElement: () => ({}),
    head: { appendChild: (script: FakeScript) => injected.push(script) },
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe('segment', () => {
  it('treats the unreplaced build placeholders as no write key', async () => {
    const { isRealSegmentKey } = await freshSegment();

    expect(isRealSegmentKey(undefined)).toBe(false);
    expect(isRealSegmentKey('CROWD_VUE_APP_SEGMENT_KEY')).toBe(false);
    expect(isRealSegmentKey('%VUE_APP_SEGMENT_KEY%')).toBe(false);
    expect(isRealSegmentKey('abc123')).toBe(true);
  });

  it('loads nothing and sends nothing when Segment is not configured', async () => {
    const { analytics } = await freshSegment();

    await analytics.track('Custom view created');

    expect(injected).toEqual([]);
    expect(calls).toEqual([]);
  });

  it('loads the LFX Segments library instead of the Segment CDN', async () => {
    const { loadSegment } = await freshSegment();

    loadSegment('write-key');

    expect(injected).toHaveLength(1);
    expect(injected[0].src).toBe(LFX_LATEST_URL);
    expect(injected[0].async).toBe(true);
  });

  it('initialises the library with the write key before forwarding calls made while it loaded', async () => {
    const { analytics, loadSegment } = await freshSegment();

    loadSegment('write-key');
    const sent = Promise.all([
      analytics.page('Members'),
      analytics.track('Custom view created', { viewName: 'All' }),
      analytics.identify('user-1', { email: 'user@example.org' }),
    ]);
    libraryLoads();
    await sent;

    expect(calls).toEqual([
      'init write-key',
      'page Members',
      'track Custom view created {"viewName":"All"}',
      'identify user-1 {"email":"user@example.org"}',
    ]);
  });

  it('keeps routing calls through the library when its first load fails, so a later consent grant can retry', async () => {
    const { analytics, loadSegment } = await freshSegment();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    fakeLibrary.init.mockRejectedValueOnce(new Error('Segment SDK failed'));

    loadSegment('write-key');
    libraryLoads();
    await analytics.track('Upgrade Plan Contact');

    expect(calls).toEqual(['track Upgrade Plan Contact']);
    expect(console.error).toHaveBeenCalledWith('Segment failed to load', expect.any(Error));
  });

  it('contains a call to a method latest/ no longer has, because no call site awaits or catches it', async () => {
    const { analytics, loadSegment } = await freshSegment();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    fakeLibrary.track.mockImplementationOnce(() => {
      throw new TypeError('instance.track is not a function');
    });

    loadSegment('write-key');
    libraryLoads();

    await expect(analytics.track('Custom view created')).resolves.toBeUndefined();
    expect(console.error).toHaveBeenCalledWith('Segment call failed', expect.any(TypeError));
  });

  it('reports a library that loads without the LfxSegmentsAnalytics global, and sends nothing', async () => {
    const { analytics, loadSegment } = await freshSegment();
    vi.spyOn(console, 'error').mockImplementation(() => undefined);

    loadSegment('write-key');
    injected[0].onload?.();

    await expect(analytics.track('Custom view created')).resolves.toBeUndefined();
    expect(calls).toEqual([]);
    expect(console.error).toHaveBeenCalledWith(
      'Segment library loaded without LfxSegmentsAnalytics',
    );
  });

  it('sends nothing when the library is blocked', async () => {
    const { analytics, loadSegment } = await freshSegment();

    loadSegment('write-key');
    const sent = analytics.track('Custom view created');
    injected[0].onerror?.();

    await expect(sent).resolves.toBeUndefined();
    expect(calls).toEqual([]);
  });
});

describe('index.html', () => {
  it('loads Transcend airgap.js synchronously before any other script', () => {
    const html = readFileSync(new URL('../../index.html', import.meta.url), 'utf8');
    const scripts = [...html.matchAll(/<script\b[^>]*>/gi)].map(([tag]) => tag);

    expect(scripts[0]).toContain(`src="${AIRGAP_URL}"`);
    expect(scripts[0]).not.toMatch(/\b(async|defer)\b|\btype\s*=\s*["']?module\b/i);
  });
});
