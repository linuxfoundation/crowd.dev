<template>
  <div class="flex flex-col">
    <lf-switch
      v-if="cForm.repositories.length > 0"
      v-model="enableAll"
      class="bg-gray-50 border-y border-gray-200 px-6 py-3 -mx-6"
    >
      <span class="text-gray-500 text-xs"> Enable all repositories </span>
    </lf-switch>
    <div
      v-for="(repository, index) of cForm.repositories"
      :key="repository.url"
      class="flex items-center py-4"
      :class="{
        'border-b border-gray-100': index !== cForm.repositories.length - 1,
        'opacity-40': !repository.enabled,
      }"
    >
      <div class="flex items-center w-3/5">
        <lf-svg
          name="git-repository"
          class="!block w-5 h-5 text-gray-900 mr-3"
        />
        <span class="text-gray-900 text-sm">{{ repository.label }}</span>
      </div>
      <div
        class="flex items-center gap-2 border border-gray-200 rounded-[100px] px-2 ml-3"
      >
        <template v-for="platform of repository.platforms" :key="platform">
          <lf-icon
            v-if="platform === 'github'"
            name="github"
            :size="25"
            class="text-[#24292F]"
            type="brands"
          />
          <lf-icon
            v-if="platform === 'git'"
            name="git-alt"
            :size="25"
            class="text-[#F05133]"
            type="brands"
          />
          <lf-svg
            v-if="platform === 'gitlab'"
            name="gitlab"
            class="!block w-4 h-4"
          />
          <img
            v-if="platform === 'gerrit'"
            :src="gerritImage"
            alt="Gerrit"
            class="min-w-5 h-5"
          />
        </template>
      </div>
      <lf-switch v-model="repository.enabled" class="ml-auto" :size="'small'" />
    </div>
    <div
      v-if="cForm.repositories.length === 0"
      class="flex items-center justify-center pt-14"
    >
      <span class="text-gray-500 text-sm">
        There are no repositories connected to this project
      </span>
    </div>
  </div>
</template>

<script setup lang="ts">
import LfSvg from '@/shared/svg/svg.vue';
import LfIcon from '@/ui-kit/icon/Icon.vue';
import LfSwitch from '@/ui-kit/switch/Switch.vue';
import { computed, reactive } from 'vue';
import type { InsightsProjectAddFormModel } from '../models/insights-project-add-form.model';

const props = defineProps<{
  form: InsightsProjectAddFormModel;
}>();

const cForm = reactive(props.form);

const enableAll = computed({
  get() {
    return cForm.repositories.every((repo) => repo.enabled);
  },
  set(value) {
    toggleRepositories(value);
  },
});

const toggleRepositories = (value: boolean) => {
  cForm.repositories = cForm.repositories.map((r) => ({
    ...r,
    enabled: value,
  }));
};

const gerritImage = new URL(
  '@/assets/images/integrations/gerrit.png',
  import.meta.url,
).href;
</script>

<script lang="ts">
export default {
  name: 'LfInsightsProjectAddRepositoriesTab',
};
</script>
