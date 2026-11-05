import { defineStore } from 'pinia';
import { ref } from 'vue';

export const useUserStore = defineStore('user', () => {
  const isDeveloperModeActive = ref<boolean>(localStorage.getItem('developerMode') === 'true');

  function updateDeveloperMode(value: boolean): void {
    isDeveloperModeActive.value = value;
    localStorage.setItem('developerMode', JSON.stringify(value));
  }

  return {
    isDeveloperModeActive,
    updateDeveloperMode,
  };
});
