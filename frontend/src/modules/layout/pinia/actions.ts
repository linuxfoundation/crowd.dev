import type { FooterState } from './state';

export default {
  setVisibility(this: FooterState, visible: boolean) {
    this.visible = visible;
  },
};
