import AuthGuard from '@/middleware/auth/auth-guard';
import UnauthGuard from '@/middleware/auth/unauth-guard';
import SegmentGuard from '@/middleware/auth/segment-guard';
import type { Guard } from '@/middleware/types';

export default [
  AuthGuard,
  UnauthGuard,
  SegmentGuard,
] satisfies Guard[];
