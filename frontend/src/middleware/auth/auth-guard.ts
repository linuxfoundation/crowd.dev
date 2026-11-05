import { useAuthStore } from '@/modules/auth/store/auth.store';
import { storeToRefs } from 'pinia';
import type { GuardContext } from '@/middleware/types';

/**
 * Auth Guard
 *
 * This middleware runs before rendering any route that has meta.auth = true
 *
 * User is authenticated, and both tenant & ser exist within our store (if not, redirects to /auth/signin)
 */

export default async function ({
  to, router,
}: GuardContext): Promise<void> {
  if (!to.meta || !to.meta.auth) {
    return;
  }
  const authStore = useAuthStore();
  const { ensureLoaded } = authStore;
  const { user } = storeToRefs(authStore);
  await ensureLoaded();
  if (!user.value || !user.value.id) {
    router.push('/auth/signin');
  }
}
