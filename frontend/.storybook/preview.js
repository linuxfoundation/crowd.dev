/** @type { import('@storybook/vue3-vite').Preview } */
import '../src/assets/scss/index.scss';
import './storybook.scss';

const preview = {
  parameters: {
    actions: { argTypesRegex: "^on[A-Z].*" },
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },
  },
};

export default preview;
