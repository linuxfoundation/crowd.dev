const SEGMENT_KEY = 'connectSegmentId';
const GRANDPARENT_KEY = 'connectSegmentGrandparentId';

export const rememberConnectSegment = (segmentId: string | null, grandparentId: string | null) => {
  if (segmentId && grandparentId) {
    sessionStorage.setItem(SEGMENT_KEY, segmentId);
    sessionStorage.setItem(GRANDPARENT_KEY, grandparentId);
  }
};

export const consumeConnectSegment = (): { segmentId: string; grandparentId: string } | null => {
  const segmentId = sessionStorage.getItem(SEGMENT_KEY);
  const grandparentId = sessionStorage.getItem(GRANDPARENT_KEY);
  sessionStorage.removeItem(SEGMENT_KEY);
  sessionStorage.removeItem(GRANDPARENT_KEY);
  return segmentId && grandparentId ? { segmentId, grandparentId } : null;
};
