import { AuthService } from '@/modules/auth/services/auth.service';

/**
 * Unauth Guard
 *
 * This middleware runs before rendering any route that has meta.unauth = true
 *
 * It checks if user is undefined (if not, redirects to /)
 *
 * @param to
 * @param router
 * @returns {Promise<*>}
 */
export default async function ({ to, router }) {
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
