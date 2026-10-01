import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/gitlab.png', import.meta.url)
  .href;

const gitlab: IdentityConfig = {
  key: 'gitlab',
  name: 'GitLab',
  image,
  member: {
    urlPrefix: 'gitlab.com/',
    url: ({ identity }) => (identity.value ? `https://gitlab.com/${identity.value}` : null),
  },
  activity: {
    showLink: true,
  },
};

export default gitlab;
