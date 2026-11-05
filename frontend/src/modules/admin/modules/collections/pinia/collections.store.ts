// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT

import { defineStore } from 'pinia';
import { ref } from 'vue';
import type { CollectionModel } from '../models/collection.model';

export const useCollectionsStore = defineStore('collections-store', () => {
  const collections = ref<CollectionModel[]>([]);

  function setCollections(value: CollectionModel[]) {
    collections.value = value;
  }

  function getCollection(id: string) {
    return collections.value.find((c) => c.id === id);
  }

  function getCollections() {
    return collections.value;
  }

  return {
    collections,
    setCollections,
    getCollection,
    getCollections,
  };
});
