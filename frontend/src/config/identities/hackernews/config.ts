import type { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/hackernews.svg',
  import.meta.url,
).href;

const hackernews: IdentityConfig = {
  key: 'hackernews',
  name: 'Hacker News',
  image,
  member: {
    urlPrefix: 'news.ycombinator.com/user?id=',
    url: ({ identity }) => (identity.value
      ? `https://news.ycombinator.com/user?id=${identity.value}`
      : null),
  },
  activity: {
    showLink: true,
  },
};

export default hackernews;
