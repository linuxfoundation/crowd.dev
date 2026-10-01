import type { User } from '@/modules/auth/types/User.type';

export interface AuthState {
  token: string | null,
  user: User | null,
  loaded: boolean,
}

export default () => ({
  user: null,
  loaded: false,
} as AuthState);
