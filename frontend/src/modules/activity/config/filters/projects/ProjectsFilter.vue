<template>
  <div
    style="max-height: 480px; overflow: auto;"
  >
    <app-lf-project-filter
      v-model:options="filteredOptions"
      :loading="loading"
      @on-change="onFilterChange"
      @on-search-change="onSearchQueryChange"
    />
  </div>
</template>

<script setup lang="ts">
import AppLfProjectFilter from '@/modules/lf/segments/components/filter/lf-project-filter.vue';
import debounce from 'lodash/debounce';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { storeToRefs } from 'pinia';
import {
  computed, onMounted, ref,
} from 'vue';
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import type {
  ProjectsFilterValue, ProjectsOption, ProjectsCustomFilterConfig, ProjectsCustomFilterOptions,
} from '@/modules/lf/segments/types/Filters';
import type { Project } from '@/modules/lf/segments/types/Segments';

const props = defineProps<
  {
    modelValue: ProjectsFilterValue;
    data: any;
    config: ProjectsCustomFilterConfig;
  } & ProjectsCustomFilterOptions
>();
const emit = defineEmits<{(e: 'update:modelValue', value: ProjectsFilterValue): void,
  (e: 'update:data', value: any): void;
}>();

const loading = ref(true);
const filteredOptions = ref<any[]>([]);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const form = computed({
  get: () => props.modelValue,
  set: (value: { value: string[], parentValues: string[] }) => emit('update:modelValue', value),
});

const data = computed({
  get: () => props.data,
  set: (value: any) => emit('update:data', value),
});

const defaultForm: ProjectsFilterValue = {
  value: [],
  parentValues: [],
};

const rules: any = {
  value: {
    required,
  },
};

useVuelidate(rules, form);

const buildOptions = (projects: Project[]) => {
  filteredOptions.value = projects.map((p) => {
    const segments = props.modelValue.value || [];
    const selectedSubProjects = p.subprojects.filter((subproject) => segments.includes(subproject.id));

    return {
      id: p.id,
      label: p.name,
      selected: selectedSubProjects.length === p.subprojects.length,
      children: p.subprojects.map((sp) => ({
        id: sp.id,
        label: sp.name,
        selected: selectedSubProjects.length !== p.subprojects.length && segments.includes(sp.id),
      })),
    };
  });
};

const onFilterChange = (value: ProjectsOption[]) => {
  let segments = [...form.value.value];
  let parentSegments = [...form.value.parentValues];

  parentSegments = value.filter((project) => project.selected).map((project) => project.id);

  value.forEach((option) => {
    if (option.selected) {
      segments = option.children.map((c) => c.id);
    } else {
      option.children.forEach((child) => {
        if (child.selected) {
          segments = [child.id];
        }
      });
    }
  });

  form.value = {
    value: segments,
    parentValues: parentSegments,
  };
};

const onSearchQueryChange = debounce((value, setDataOptions = false) => {
  loading.value = true;
  props
    .remoteMethod?.({
      query: value,
      parentSlug: selectedProjectGroup.value?.slug,
    })
    .then((projects) => {
      if (setDataOptions) {
        data.value.options = projects;
      }
      buildOptions(projects);
    })
    .finally(() => {
      loading.value = false;
    });
}, 300);

onMounted(() => {
  onSearchQueryChange('', true);
  emit('update:modelValue', {
    ...defaultForm,
    ...form.value,
  });
});
</script>
