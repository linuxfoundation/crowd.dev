<template>
  <lf-github-mappings-display :mappings="mappings" />
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import LfGithubMappingsDisplay from '@/config/integrations/github/components/github-mappings-display.vue';
import { IntegrationMapping } from '@/modules/admin/modules/integration/types/Integration';

const props = defineProps<{
  integration: any;
}>();

const mappings = ref<IntegrationMapping[]>([]);

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
  name: 'LfGithubStatus',
};
</script>
