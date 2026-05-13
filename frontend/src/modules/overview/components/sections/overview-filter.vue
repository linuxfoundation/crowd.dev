<template>
  <div class="flex items-center">
    <app-lf-project-group-filter />
    <app-lf-project-filter v-if="selectedProjectGroup && projects.length > 0" :projects="projects" />
    <app-lf-sub-project-filter v-if="selectedProject" :project-id="selectedProjectId" />
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { storeToRefs } from 'pinia';
import AppLfProjectGroupFilter from '../fragments/project-group-filter.vue';
import AppLfProjectFilter from '../fragments/project-filter.vue';
import AppLfSubProjectFilter from '../fragments/sub-project-filter.vue';
import { useOverviewStore } from '../../store/overview.store';

const overviewStore = useOverviewStore();
const { selectedProjectGroup, selectedProject, selectedProjectId } = storeToRefs(overviewStore);

const projects = computed(() => selectedProjectGroup.value?.projects || []);
</script>

<script lang="ts">
export default {
  name: 'AppLfOverviewFilter',
};
</script>
