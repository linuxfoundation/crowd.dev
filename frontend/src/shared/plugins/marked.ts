import type { App } from 'vue';
import { marked } from 'marked';

export default {
  install: (app: App): void => {
    // eslint-disable-next-line no-param-reassign
    app.config.globalProperties.$marked = (
      markdownString: string,
      options: Omit<marked.MarkedOptions, 'async'> = {},
    ): string => {
      marked.setOptions(options);

      return marked.parse(markdownString);
    };
  },
};
