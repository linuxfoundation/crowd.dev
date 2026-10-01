import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/github.png', import.meta.url)
  .href;

const github: IdentityConfig = {
  key: 'github',
  name: 'GitHub',
  image,
  icon: 'github',
  iconType: 'brands',
  color: '#24292F',
  member: {
    urlPrefix: 'github.com/',
    url: ({ identity }) => (identity.value ? `https://github.com/${identity.value}` : null),
  },
  organization: {
    urlPrefix: 'github.com/',
  },
  activity: {
    showLink: true,
  },
};

export default github;
