<template>
  <div>
    <div class="flex items-center gap-1">
      <el-popover trigger="hover" placement="top" popper-class="!w-auto">
        <template #reference>
          <div
            class="text-gray-600 text-2xs flex items-center leading-5 font-semibold"
          >
            <lf-svg name="git-repository" class="w-4 h-4 !text-gray-600 mr-1 flex items-center" />
            {{ pluralize("repository", Object.keys(mappings).length, true) }}
          </div>
        </template>

        <p class="text-gray-400 text-sm font-semibold mb-4">
          GitLab repositories
        </p>
        <div class="-my-1 px-1 max-h-44 overflow-auto">
          <article
            v-for="mapping of mappings"
            :key="mapping.url"
            class="py-2 flex items-center flex-nowrap"
          >
            <lf-svg name="git-repository" class="w-4 h-4 mr-2 flex items-center" />
            <a
              :href="mapping.url"
              target="_blank"
              rel="noopener noreferrer"
              class="text-xs leading-5 max-w-3xs truncate hover:underline"
            >
              /{{ repoNameFromUrl(mapping?.url) }}
            </a>
            <lf-icon name="arrow-right" :size="16" class="text-gray-400 mx-2 flex items-center" />
            <div class="text-xs leading-5 max-w-3xs truncate">
              {{ mapping?.segment?.name }}
            </div>
          </article>
        </div>
      </el-popover>

      <el-tooltip
        content="Only public repositories will be tracked."
        placement="top"
      >
        <lf-icon name="circle-info" :size="13" class="text-gray-600" />
      </el-tooltip>
    </div>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue';
import { IntegrationService } from '@/modules/integration/integration-service';
import pluralize from 'pluralize';
import LfSvg from '@/shared/svg/svg.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const props = defineProps({
  integration: {
    type: Object,
    default: () => {},
  },
});

const mappings = ref([]);

const repoNameFromUrl = (url) => url.split('/').at(-1);

onMounted(() => {
  IntegrationService.fetchGitLabMappings(props.integration).then((res) => {
    mappings.value = res;
  });
});
</script>

<script>
export default {
  name: 'AppGitlabSettings',
};
</script>
