<template>
  <lf-github-mappings-display v-if="!isInProgress" :mappings="mappings" />
  <lf-github-mappings-display v-else :mappings="mappedRepositories">
    <template #trigger>
      <span
        class="text-gray-600 text-xs flex items-center leading-5 font-semibold underline decoration-dotted cursor-default"
      >
        {{ mappedRepositories.length }} out of
        {{ allRepositoriesNames.length }} repositories connected
      </span>
    </template>
    <template #popupTitle>
      Connected GitHub repositories
    </template>
  </lf-github-mappings-display>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import LfGithubMappingsDisplay from '@/config/integrations/github/components/github-mappings-display.vue';
import { IntegrationMapping } from '@/modules/admin/modules/integration/types/Integration';

const props = defineProps<{
  integration: any;
}>();

const mappings = ref<IntegrationMapping[]
>([]);

const repoNameFromUrl = (url: string) => url.split('/').at(-1);

const mappedRepositories = computed(() => {
  const reposObj = props.integration.settings.nangoMapping;
  const mapped = reposObj
    ? Object.values(reposObj).map((repo: any) => repo.repoName) || []
    : [];

  return mappings.value.filter((mapping) => mapped.includes(repoNameFromUrl(mapping.url)));
});

const allRepositoriesNames = computed(() => props.integration.settings.orgs
  .map((org: any) => org.repos.map((repo: any) => repo.name))
  .flat());

const isInProgress = computed(() => mappedRepositories.value.length < allRepositoriesNames.value.length);
onMounted(() => {
  if (props.integration.status !== 'mapping') {
    IntegrationService.fetchGitHubMappings(props.integration).then((res) => {
      mappings.value = res;
    });
  }
});
</script>

<script lang="ts">
export default {
  name: 'LfGithubParams',
};
</script>
