import type { Preview } from '@storybook/vue3';
import '../src/assets/scss/index.scss';
import './storybook.scss';

const preview: Preview = {
  parameters: {
    controls: { matchers: { color: /(background|color)$/i, date: /Date$/i } },
  },
};

export default preview;
