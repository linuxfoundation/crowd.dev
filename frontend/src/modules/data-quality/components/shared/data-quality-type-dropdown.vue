<template>
  <lf-dropdown width="21.25rem">
    <template #trigger>
      <lf-button type="secondary">
        <lf-icon name="message-exclamation" :size="16" />
        <div class="truncate" style="max-width: 40ch;">
          <span>Issue type: </span>
          <span v-if="selectedType === 'merge-suggestions'" class="font-normal">Merge suggestions</span>
          <span v-else-if="selectedType === 'bot-suggestions'" class="font-normal">Bot suggestions</span>
          <span v-else-if="selectedType === 'fake-suggestions'" class="font-normal">Fake organization suggestions</span>
          <span v-else class="font-normal">{{ dataIssueTypes[selectedType]?.label || 'Select issue type' }}</span>
        </div>
      </lf-button>
    </template>
    <div class="flex flex-col gap-2">
      <lf-dropdown-item
        :selected="selectedType === 'merge-suggestions'"
        class="!text-small"
        @click="selectedType = 'merge-suggestions'"
      >
        Merge suggestions
      </lf-dropdown-item>
      <lf-dropdown-item
        v-for="type in suggestionTypes"
        :key="type"
        :selected="selectedType === type"
        class="!text-small"
        @click="selectedType = type"
      >
        {{ suggestionTypeLabels[type] }}
      </lf-dropdown-item>
      <section
        v-for="(section, si) in props.config"
        :key="si"
        class="border-t border-gray-100 -mx-2 px-2"
      >
        <p class="text-small text-gray-400 font-semibold p-2 leading-5">
          {{ section.label }}
        </p>
        <div class="flex flex-col gap-1">
          <lf-dropdown-item
            v-for="(type, ti) in section.types"
            :key="ti"
            :selected="selectedType === type"
            class="!text-small"
            @click="selectedType = type"
          >
            {{ dataIssueTypes[type]?.label }}
          </lf-dropdown-item>
        </div>
      </section>
    </div>

    <!--    <lf-dropdown-item-->
    <!--      v-for="type in props.types"-->
    <!--      :key="type"-->
    <!--      :selected="selectedType === type"-->
    <!--      @click="selectedType = type"-->
    <!--    >-->
    <!--      {{ dataIssueTypes[type]?.label }}-->
    <!--    </lf-dropdown-item>-->
  </lf-dropdown>
</template>

<script lang="ts" setup>
import LfButton from '@/ui-kit/button/Button.vue';
import { computed, withDefaults } from 'vue';
import LfDropdown from '@/ui-kit/dropdown/Dropdown.vue';
import LfDropdownItem from '@/ui-kit/dropdown/DropdownItem.vue';
import { DataIssueTypeMenu, dataIssueTypes } from '@/modules/data-quality/config/data-issue-types';
import LfIcon from '@/ui-kit/icon/Icon.vue';

const suggestionTypeLabels: Record<string, string> = {
  'bot-suggestions': 'Bot suggestions',
  'fake-suggestions': 'Fake organization suggestions',
};

const props = withDefaults(defineProps<{
  modelValue: string;
  config: DataIssueTypeMenu[]
  suggestionTypes?: string[]
}>(), {
  suggestionTypes: () => ['bot-suggestions'],
});

const emit = defineEmits<{(e: 'update:modelValue', value: string): void}>();

const selectedType = computed({
  get: () => props.modelValue,
  set: (value: string) => emit('update:modelValue', value),
});

</script>

<script lang="ts">
export default {
  name: 'LfDataQualityTypeDropdown',
};
</script>
