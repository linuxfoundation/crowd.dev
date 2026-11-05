<template>
  <lf-modal
    v-model="isModalVisible"
    content-class="!overflow-unset"
    width="42rem"
    header-title="Bulk selection"
  >
    <div class="px-6">
      <article class="pb-3">
        <p class="text-sm font-medium text-black mb-1">
          Repositories
        </p>
        <el-select
          v-model="form.repositories"
          placeholder="Select option(s)"
          class="w-full"
          placement="bottom-end"
          filterable
          multiple
          :teleported="false"
        >
          <el-option value="all" label="All repositories" @click="selectAll" />
          <template
            v-for="(projects, owner) in props.repositories"
            :key="owner"
          >
            <el-option-group :label="owner">
              <el-option
                v-for="project in projects"
                :key="project.web_url"
                :value="project.web_url"
                :label="project.name || project.path_with_namespace"
              />
            </el-option-group>
          </template>
        </el-select>
      </article>
      <article class="pb-8">
        <p class="text-sm font-medium text-black mb-1">
          Map with
        </p>
        <el-select
          v-model="form.segment"
          placeholder="Select segment"
          class="w-full"
          placement="bottom-end"
          filterable
          :teleported="false"
        >
          <el-option
            v-for="segment of props.subprojects"
            :key="segment.id"
            :value="segment.id"
            :label="segment.name"
          />
        </el-select>
      </article>
    </div>
    <div class="bg-gray-50 px-6 py-4 flex items-center justify-end gap-4">
      <lf-button type="bordered" size="medium" @click="closeModal">
        Cancel
      </lf-button>
      <lf-button type="primary" :disabled="$v.$invalid" @click="applyMapping">
        <span>
          Map repositories
          <span v-if="form.repositories.length > 0">({{ selectedReposCount }})</span>
        </span>
      </lf-button>
    </div>
  </lf-modal>
</template>

<script setup lang="ts">
import type { SubProject } from '@/modules/lf/segments/types/Segments';
import type { GitlabMappedProject } from '@/config/integrations/gitlab/types/GitlabSettings';
import { computed, reactive } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';

const props = defineProps<{
  modelValue: boolean;
  repositories: Record<string, GitlabMappedProject[]>;
  subprojects: SubProject[];
  mappedRepos: Record<string, string>;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'apply', value: Record<string, string>): void;
}>();

const isModalVisible = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emit('update:modelValue', val);
  },
});

const form = reactive<{
  segment: string;
  repositories: string[];
}>({
  segment: '',
  repositories: [],
});

const rules = {
  segment: {
    required,
  },
  repositories: {
    required,
  },
};

const $v = useVuelidate(rules, form, { $stopPropagation: true });

const selectedReposCount = computed(() => {
  if (form.repositories.includes('all')) {
    return Object.values(props.repositories).flat().length;
  }
  return form.repositories.length;
});

const selectAll = () => {
  if (form.repositories.includes('all')) {
    form.repositories = ['all'];
  } else {
    form.repositories = Object.values(props.repositories)
      .flat()
      .map((project) => project.web_url);
  }
};

const closeModal = () => {
  isModalVisible.value = false;
  form.repositories = [];
  form.segment = '';
  $v.value.$reset();
};

const applyMapping = () => {
  let repos = form.repositories;
  if (repos.includes('all')) {
    repos = Object.values(props.repositories)
      .flat()
      .map((project) => project.web_url);
  }
  const data = repos.reduce(
    (mapping, url) => ({
      ...mapping,
      [url]: form.segment,
    }),
    {},
  );
  emit('apply', data);
  closeModal();
};
</script>

<script lang="ts">
export default {
  name: 'AppGitlabSettingsBulkSelect',
};
</script>
