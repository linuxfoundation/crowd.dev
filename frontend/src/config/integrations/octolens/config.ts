import type { IntegrationConfig } from '@/config/integrations';

const image = new URL('@/assets/images/integrations/octolens.png', import.meta.url).href;

// Must match ENABLED_PLATFORMS in services/libs/connectors/src/connectors/octolens/mapper.ts.
export const octolensMentionPlatforms: readonly string[] = [
  'github',
  'twitter',
  'linkedin',
  'reddit',
  'devto',
  'hackernews',
  'bluesky',
  'tiktok',
  'instagram',
  'youtube',
];

const octolens: IntegrationConfig = {
  key: 'octolens',
  name: 'Octolens',
  image,
  description:
    'Mentions of LF projects on GitHub, X/Twitter, LinkedIn, Reddit, Bluesky and more, matched to projects automatically.',
  showProgress: false,
  autoManaged: true,
};

export default octolens;
