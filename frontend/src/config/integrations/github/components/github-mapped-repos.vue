<template>
  <!-- Mapped repos with other projects -->
  <div
    v-if="!!mappedReposWithOtherProject?.repositories?.length"
    class="items-center py-2.5 px-4 flex justify-between bg-gray-50"
  >
    <span class="text-neutral-600 text-2xs flex items-center leading-5">
      <lf-icon name="arrows-rotate" :size="16" class="mr-2" />
      Syncing GitHub data from <span class="font-semibold px-1"> {{ projectNameDisplay }} </span>
      for&nbsp;

      <lf-github-mappings-display :mappings="projectReposAsMappings">
        <template #trigger>
          <span class="underline decoration-dotted cursor-default">
            {{ pluralize("repository", projectReposAsMappings.length, true) }}
          </span>
        </template>
      </lf-github-mappings-display>
    </span>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, computed } from 'vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfGithubMappingsDisplay from '@/config/integrations/github/components/github-mappings-display.vue';
import { IntegrationMapping } from '@/modules/admin/modules/integration/types/Integration';
import pluralize from 'pluralize';

const props = defineProps<{
  segmentId: string;
}>();

const mappedReposWithOtherProject = ref<{
  projects: {
    segmentName: string;
    url: string;
  }[];
  repositories: {
    url: string;
  }[];
}>();

const projectReposAsMappings = computed<IntegrationMapping[]>(() => {
  if (!mappedReposWithOtherProject.value?.projects) return [];

  return mappedReposWithOtherProject.value?.projects.map((proj) => ({
    url: proj.url,
    sourcePlatform: '',
    segment: {
      id: '',
      name: proj.segmentName,
    },
  }));
});

const projectNames = computed(() => mappedReposWithOtherProject.value?.projects.reduce((acc, proj) => {
  acc[proj.segmentName] = proj.segmentName;
  return acc;
}, {} as { [key: string]: string }));

const projectNameDisplay = computed(() => {
  const count = projectNames.value ? Object.keys(projectNames.value).length : 0;

  if (count > 1) return `${count} projects`;

  return count === 1 ? `${mappedReposWithOtherProject.value?.projects[0].segmentName} project` : 'projects';
});

onMounted(() => {
  IntegrationService.fetchGitHubMappedRepos(props.segmentId).then((res) => {
    mappedReposWithOtherProject.value = res;
  });
});
</script>

<script lang="ts">
export default {
  name: 'LfGithubMappedRepos',
};
</script>
