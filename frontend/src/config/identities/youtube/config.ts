import type { IdentityConfig } from '@/config/identities';

const image = new URL('@/assets/images/identities/youtube.svg', import.meta.url).href;

const youtube: IdentityConfig = {
  key: 'youtube',
  name: 'YouTube',
  image,
  member: {
    urlPrefix: 'youtube.com/channel/',
    url: ({ identity }) =>
      identity.value ? `https://www.youtube.com/channel/${identity.value}` : null,
  },
  activity: {
    showLink: true,
  },
};

export default youtube;
