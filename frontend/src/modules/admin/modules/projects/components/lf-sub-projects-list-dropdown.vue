<template>
  <app-form-item
    class="mb-0"
    :validation="$v.subprojectId"
    label="Sub-project"
    :required="true"
    :error-messages="{
      required: 'Sub-project is required',
    }"
  >
    <el-input
      ref="inputRef"
      v-model="inputValue"
      class="subprojects-select-input"
      placeholder="Select option"
      readonly
      :suffix-icon="isPopoverVisible ? ArrowUpIcon : ArrowDownIcon"
      @blur="$v.subprojectId.$touch"
      @click="isPopoverVisible = true"
    />
  </app-form-item>

  <el-popover
    v-model:visible="isPopoverVisible"
    :virtual-ref="inputRef"
    placement="bottom"
    trigger="manual"
    virtual-triggering
    popper-class="subprojects-select-popper"
    :teleported="false"
  >
    <div class="mb-2 border-b border-gray-100 px-2 pt-2 pb-1 w-full">
      <el-input
        id="filterSearch"
        v-model="searchQuery"
        placeholder="Search..."
        class="filter-dropdown-search"
        :prefix-icon="SearchIcon"
        @input="onSearchProjects"
      />
    </div>

    <div class="p-2">
      <div v-if="projectsList.length" class="flex flex-col gap-4">
        <div
          v-for="project in projectsList"
          :key="project.id"
        >
          <div class="uppercase text-gray-400 font-semibold text-2xs tracking-widest px-3 pb-3">
            {{ project.name }}
          </div>
          <div class="flex flex-col gap-1">
            <div
              v-for="subproject of project.subprojects"
              :key="subproject.id"
              class="h-10 flex items-center px-3 text-xs text-gray-900 hover:bg-gray-50 rounded cursor-pointer"
              :class="{
                'bg-primary-50': subproject.id === form.subprojectId,
              }"
              @click="onOptionClick(subproject, project)"
            >
              {{ subproject.name }}
            </div>
          </div>
        </div>
      </div>
      <div v-else class="text-gray-400 px-3 h-10 flex items-center">
        No projects found
      </div>
    </div>
  </el-popover>
</template>

<script setup>
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import {
  reactive, h, ref, onMounted,
} from 'vue';
import AppFormItem from '@/shared/form/form-item.vue';
import { storeToRefs } from 'pinia';
import { useLfSegmentsStore } from '@/modules/lf/segments/store';
import { LfService } from '@/modules/lf/segments/lf-segments-service';

const props = defineProps({
  selectedSubproject: {
    type: Object,
    default: () => {},
  },
  selectedSubprojectParent: {
    type: Object,
    default: () => {},
  },
});

const emit = defineEmits(['onChange']);

const SearchIcon = h(
  'i', // type
  { class: 'fa-light fa-magnifying-glass c-icon' }, // props
  [],
);

const ArrowDownIcon = h(
  'i', // type
  { class: 'fa-light fa-chevron-down c-icon' }, // props
  [],
);

const ArrowUpIcon = h(
  'i', // type
  { class: 'fa-light fa-chevron-up c-icon' }, // props
  [],
);

const lsSegmentsStore = useLfSegmentsStore();
const { selectedProjectGroup } = storeToRefs(lsSegmentsStore);

const inputRef = ref(null);
const loading = ref(false);
const searchQuery = ref('');
const projectsList = ref([]);
const isPopoverVisible = ref(false);

const inputValue = ref('');
const form = reactive({
  subprojectId: '',
});

const rules = {
  subprojectId: {
    required,
  },
};

const $v = useVuelidate(rules, form);

const listProjects = (clearList) => {
  loading.value = true;

  LfService.queryProjects({
    filter: {
      name: searchQuery.value,
      parentSlug: selectedProjectGroup.value.slug,
    },
  })
    .then((response) => {
      if (clearList) {
        projectsList.value = response.rows;
      } else {
        projectsList.value = projectsList.value.concat(response.rows);
      }
    })
    .finally(() => {
      loading.value = false;
    });
};

const getInputValue = (subproject, project) => `${subproject.name} (${project.name})`;

onMounted(() => {
  listProjects();

  if (props.selectedSubproject && props.selectedSubprojectParent) {
    form.subprojectId = props.selectedSubproject.id;
    inputValue.value = getInputValue(props.selectedSubproject, props.selectedSubprojectParent);
  }
});

const onSearchProjects = (query) => {
  searchQuery.value = query;

  listProjects(true);
};

const onOptionClick = (subproject, project) => {
  isPopoverVisible.value = false;

  form.subprojectId = subproject.id;
  inputValue.value = getInputValue(subproject, project);

  emit('onChange', {
    subprojectId: form.subprojectId,
    isSubmitEnabled: !$v.value.$invalid,
  });
};
</script>

<script>
export default {
  name: 'AppLfSubProjectsListDropdown',
};
</script>

<style lang="scss">
.subprojects-select-popper.el-popper {
    width: 100% !important;
    max-height: 480px;
    overflow: auto;
    @apply p-0;
}

.subprojects-select-input {
    @apply cursor-pointer relative w-full;

    .el-input__inner {
        @apply cursor-pointer
    }
}
</style>
