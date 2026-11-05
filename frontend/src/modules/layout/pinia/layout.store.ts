// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
import { defineStore } from 'pinia';
import { ref } from 'vue';

export const useLayoutStore = defineStore('layout', () => {
  const menuCollapsed = ref(false);

  function toggleMenu() {
    menuCollapsed.value = !menuCollapsed.value;
  }

  return { menuCollapsed, toggleMenu };
});
