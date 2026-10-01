import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/devto.png', import.meta.url)
  .href;

const devto: IdentityConfig = {
  key: 'devto',
  name: 'DEV',
  image,
  member: {
    urlPrefix: 'dev.to/',
    url: ({ identity }) => (identity.value ? `https://dev.to/${identity.value}` : null),
  },
  activity: {
    showLink: true,
  },
};

export default devto;
