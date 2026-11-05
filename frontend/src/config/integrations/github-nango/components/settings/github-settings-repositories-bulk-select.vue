<template>
  <lf-modal v-model="isModalVisible" content-class="!overflow-unset" width="42rem" header-title="Bulk selection">
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
          <el-option
            value="all"
            label="All repositories"
            @click="form.repositories = ['all']"
          />
          <el-option
            v-for="repo of props.repositories"
            :key="repo.url"
            :value="repo.url"
            :label="repo.name"
            @click="
              isAll
                ? (form.repositories = form.repositories.filter(
                  (r) => r !== 'all',
                ))
                : null
            "
          />
        </el-select>
      </article>
      <article class="pb-8">
        <p class="text-sm font-medium text-black mb-1">
          Map with
        </p>
        <el-select
          v-model="form.subproject"
          placeholder="Select sub-project"
          class="w-full"
          placement="bottom-end"
          filterable
          :teleported="false"
        >
          <el-option
            v-for="subproject of props.subprojects"
            :key="subproject.id"
            :value="subproject.id"
            :label="subproject.name"
          />
        </el-select>
      </article>
    </div>
    <div class="bg-gray-50 px-6 py-4 flex items-center justify-end gap-4">
      <lf-button
        type="bordered"
        size="medium"
        @click="
          isModalVisible = false;
          reset();
        "
      >
        Cancel
      </lf-button>
      <lf-button type="primary" :disabled="$v.$invalid" @click="mapRepos()">
        <span>
          Map repositories
          <span v-if="form.repositories.length > 0">({{
            isAll ? props.repositories.length : form.repositories.length
          }})</span>
        </span>
      </lf-button>
    </div>
  </lf-modal>
</template>

<script lang="ts" setup>
import type { SubProject } from '@/modules/lf/segments/types/Segments';
import type { GitHubSettingsRepository } from '@/config/integrations/github-nango/types/GithubSettings';
import { computed, reactive } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfModal from '@/ui-kit/modal/Modal.vue';
import { required } from '@vuelidate/validators';
import useVuelidate from '@vuelidate/core';

const props = defineProps<{
  modelValue: boolean;
  repositories: GitHubSettingsRepository[];
  subprojects: SubProject[];
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'apply', value: Record<string, string>): void;
}>();

// Modal visibility
const isModalVisible = computed({
  get() {
    return props.modelValue;
  },
  set(val) {
    emit('update:modelValue', val);
  },
});

const form = reactive<{
  subproject: string;
  repositories: string[];
}>({
  subproject: '',
  repositories: [],
});

const rules = {
  subproject: {
    required,
  },
  repositories: {
    required,
  },
};

const $v = useVuelidate(rules, form, { $stopPropagation: true });

const isAll = computed<boolean>(() => form.repositories.includes('all'));

const reset = () => {
  form.subproject = '';
  form.repositories = [];
  $v.value.$reset();
};

const mapRepos = () => {
  let repos = form.repositories;
  if (isAll.value) {
    repos = props.repositories.map((r) => r.url);
  }
  const data = repos.reduce(
    (mapping, url) => ({
      ...mapping,
      [url]: form.subproject,
    }),
    {},
  );
  emit('apply', data);
  reset();
  isModalVisible.value = false;
};
</script>

<script lang="ts">
export default {
  name: 'LfGithubSettingsRepositoriesBulkSelect',
};
</script>
