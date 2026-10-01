<template>
  <span class="text-gray-500 text-sm"> {{ copy }}</span>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type {
  SavedViewsConfig, DefaultFiltersSettings,
} from '@/shared/modules/saved-views/types/SavedViewsConfig';

const props = defineProps<{
  config: SavedViewsConfig;
  settings: DefaultFiltersSettings;
}>();

const copy = computed(() => props.config.defaultFilters.render(props.settings));
</script>
