import { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/sched.png', import.meta.url).href;

const sched: IdentityConfig = {
  key: 'sched',
  name: 'Sched',
  image,
  member: {
    placeholder: 'Sched username or email address',
  },
};

export default sched;
