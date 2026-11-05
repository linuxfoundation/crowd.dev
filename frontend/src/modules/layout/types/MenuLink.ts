import type { RouteLocationRaw } from 'vue-router';
import type { User } from '@/modules/user/types/User';

interface MenuLinkParams {
  user: User,
}

export interface MenuLink {
  id: string;
  label: string;
  icon?: string;
  routeName?: string;
  routeOptions?: RouteLocationRaw;
  href?: string;
  click?: () => void;
  display: (params: MenuLinkParams) => boolean;
}
