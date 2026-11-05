import { defineStore } from 'pinia';
import { ref } from 'vue';

export const useFooterStore = defineStore('footer-store', () => {
  const visible = ref<boolean>(true);

  function setVisibility(value: boolean): void {
    visible.value = value;
  }

  return {
    visible,
    setVisibility,
  };
});
