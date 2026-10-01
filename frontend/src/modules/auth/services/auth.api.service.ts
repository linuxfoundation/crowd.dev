import authAxios from '@/shared/axios/auth-axios';
import type { User } from '@/modules/auth/types/User.type';

class AuthApiServiceClass {
  ssoGetToken(idToken: string): Promise<any> {
    return authAxios
      .post('/auth/sso/callback', {
        idToken,
      })
      .then((response) => response.data);
  }

  fetchMe(): Promise<User> {
    return authAxios
      .get('/auth/me', {
        params: {
          excludeSegments: true,
        },
      }).then((response) => response.data);
  }
}

export const AuthApiService = new AuthApiServiceClass();
