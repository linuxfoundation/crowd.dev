<template>
  <div v-if="loading" class="h-10 px-2 flex items-center">
    <span class="text-gray-400">Loading</span>
  </div>
  <ul v-else-if="model.length" class="lf-radio">
    <li v-for="option in model" :key="option.label">
      <el-radio-group
        v-model="selectedProject"
        class="px-2"
        @change="(value) => handleParentOptionChange(value)"
      >
        <el-radio
          :label="option.id"
          class="h-10 !flex !items-center"
        >
          <span class="text-gray-900 text-xs font-semibold">{{
            option.label
          }}</span>
        </el-radio>
        <div v-if="option.children">
          <el-radio-group
            v-model="selectedSubProject"
            @change="(value) => handleChildOptionChange(value, option)"
          >
            <el-radio
              v-for="(childOption, index) in option.children"
              :key="index"
              :label="childOption.id"
              class="h-10 !pl-5 !flex !items-center"
            >
              <span class="text-gray-900 text-xs">{{ childOption.label }}</span>
            </el-radio>
          </el-radio-group>
        </div>
      </el-radio-group>
    </li>
  </ul>
  <div v-else-if="emptyText" class="h-10 px-2 flex items-center">
    <span class="text-gray-400">{{ emptyText }}</span>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue';

const props = defineProps({
  options: {
    type: Array,
    default: () => [],
  },
  emptyText: {
    type: String,
    default: null,
  },
  loading: {
    type: Boolean,
    default: false,
  },
});
const emit = defineEmits(['update:options', 'onChange']);
const model = computed({
  get() {
    return props.options;
  },
  set(v) {
    emit('update:options', v);
    emit('onChange', v);
  },
});

const selectedProject = ref('');
const selectedSubProject = ref('');

const handleParentOptionChange = (parentOptionValue) => {
  if (parentOptionValue) {
    selectedSubProject.value = '';
  }

  model.value = props.options.map((option) => ({
    ...option,
    selected: option.id === parentOptionValue,
    children: option.children.map((c) => ({
      ...c,
      selected: false,
    })),
  }));
};

const handleChildOptionChange = (childOptionValue) => {
  if (childOptionValue) {
    selectedProject.value = '';
  }

  model.value = props.options.map((option) => ({
    ...option,
    selected: false,
    children: option.children.map((c) => ({
      ...c,
      selected: c.id === childOptionValue,
    })),
  }));
};

watch(() => props.options, (updatedOptions) => {
  selectedProject.value = updatedOptions.find((option) => option.selected)?.id;
  selectedSubProject.value = updatedOptions.flatMap((option) => option.children).find((child) => child.selected)?.id;
}, {
  deep: true,
  immediate: true,
});
</script>

<script>
export default {
  name: 'AppLfRadioCascader',
};
</script>

<style lang="scss">
.lf-radio .el-radio-group {
  @apply flex flex-col items-start;
}
</style>
