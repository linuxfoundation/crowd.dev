import { IdentityConfig } from '@/config/identities';

const image = new URL(
  '@/assets/images/identities/mailing-list.svg',
  import.meta.url,
).href;

const mailinglist: IdentityConfig = {
  key: 'mailinglist',
  name: 'Mailing List',
  image,
  member: {
    placeholder: 'Mailing list email address',
  },
  activity: {
    showLink: true,
  },
};

export default mailinglist;
