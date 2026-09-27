import type { App } from 'vue';
import { marked } from 'marked';

export default {
  install: (app: App) => {
    // eslint-disable-next-line no-param-reassign
    app.config.globalProperties.$marked = (
      markdownString: string,
      options: marked.MarkedOptions = {},
    ) => {
      marked.setOptions(options);

      return marked.parse(markdownString);
    };
  },
};
