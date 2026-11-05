<template>
  <div v-if="selectedItems.length > 0" class="app-list-table-bulk-actions">
    <span class="block text-sm font-semibold mr-4">
      {{ pluralize("person", selectedItems.length, true) }}
      selected</span>

    <slot />
  </div>
</template>

<script setup lang="ts">
import pluralize from 'pluralize';

defineProps<{
  selectedItems: unknown[];
}>();
</script>

<script lang="ts">
export default {
  name: 'LfTableBulkActions',
};
</script>
