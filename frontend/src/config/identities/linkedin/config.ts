import type { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/linkedin.png',
  import.meta.url,
).href;

const linkedin: IdentityConfig = {
  key: 'linkedin',
  name: 'LinkedIn',
  image,
  icon: 'linkedin',
  iconType: 'brands',
  color: '#2867B2',
  member: {
    urlPrefix: 'linkedin.com/in/',
    url: ({ identity }) => (!identity.value?.includes('private-')
      ? `https://linkedin.com/in/${identity.value}`
      : null),
  },
  organization: {
    urlPrefix: 'linkedin.com/company/',
  },
  activity: {
    showLink: true,
  },
};

export default linkedin;
