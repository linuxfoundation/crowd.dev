import { useLogRocket } from '@/utils/logRocket';
import type { User } from '@/modules/auth/types/User.type';
import { analytics } from '@/utils/segment';

export default function identify(user: User): void {
  const { identify } = useLogRocket();

  analytics.identify(user.id, {
    name: user.fullName,
    firstName: user.firstName,
    lastName: user.lastName,
    email: user.email,
    createdAt: user.createdAt,
  });

  identify(user);
}
