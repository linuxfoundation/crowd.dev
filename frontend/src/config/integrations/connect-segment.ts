export const rememberConnectSegment = (segmentId: string | null, grandparentId: string | null) => {
  if (segmentId && grandparentId) {
    localStorage.setItem('segmentId', segmentId);
    localStorage.setItem('segmentGrandparentId', grandparentId);
  }
};
