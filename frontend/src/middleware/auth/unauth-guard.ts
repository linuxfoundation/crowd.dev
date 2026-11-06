import { AuthService } from '@/modules/auth/services/auth.service';
import type { GuardContext } from '@/middleware/types';

/**
 * Unauth Guard
 *
 * This middleware runs before rendering any route that has meta.unauth = true
 *
 * It checks if user is undefined (if not, redirects to /)
 */
export default async function ({ to, router }: GuardContext): Promise<void> {
  if (!to.meta || !to.meta.unauth) {
    return;
  }

  const token = AuthService.getToken();

  if (token) {
    // `window.history.replaceState` to replace the current URL with the root URL
    window.history.replaceState(null, '', '/');

    // `window.history.pushState` to add the root URL to the history stack
    window.history.pushState(null, '', '/');

    router.push('/');
  }
}
