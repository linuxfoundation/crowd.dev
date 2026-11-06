<template>
  <div ref="root" class="app-keywords-input">
    <div
      class="el-keywords-input-wrapper"
      :class="focused ? 'is-focus' : ''"
      @click="focusKeywordInput"
    >
      <lf-tag
        v-for="(keyword, idx) in innerKeywords"
        v-bind="$attrs"
        :key="idx"
        size="medium"
        type="secondary"
        :closable="!readOnly"
        @close="remove(idx)"
      >
        {{ keyword }}
      </lf-tag>
      <input
        v-if="!readOnly"
        class="el-keywords-input"
        :placeholder="placeholder ?? undefined"
        :value="newKeyword"
        autocomplete="off"
        data-lpignore="true"
        @input="inputKeyword"
        @keydown.delete.stop="removeLastKeyword"
        @keydown="addNew"
        @focus="focus"
        @blur="blur"
      />
    </div>
    <div
      v-if="!isError"
      class="flex items-center mt-2"
      :class="focused || showHint ? 'opacity-100' : 'opacity-0'"
    >
      <span class="helper-copy">{{ hintText }}</span>
    </div>
    <div v-else class="flex items-center mt-2">
      <slot name="error" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue';
import LfTag from '@/ui-kit/tag/Tag.vue';

defineOptions({ name: 'AppKeywordsInput' });

const props = withDefaults(defineProps<{
  modelValue?: string[];
  addKeywordOnKeys?: number[];
  readOnly?: boolean;
  placeholder?: string | null;
  isError?: boolean;
  showHint?: boolean;
  hintText?: string;
}>(), {
  modelValue: () => [],
  addKeywordOnKeys: () => [13, 188, 9],
  readOnly: false,
  placeholder: null,
  isError: false,
  showHint: false,
  hintText: 'Press ENTER or comma (,) to separate keywords.',
});

const emit = defineEmits<{(e: 'update:modelValue', value: string[]): void}>();

const root = ref<HTMLElement | null>(null);
const newKeyword = ref('');
const innerKeywords = ref<string[]>([...props.modelValue]);
const focused = ref(false);

watch(() => props.modelValue, () => {
  innerKeywords.value = [...props.modelValue];
});

const focusKeywordInput = () => {
  if (props.readOnly) {
    return;
  }
  const input = root.value?.querySelector<HTMLInputElement>('.el-keywords-input');
  if (input) {
    input.focus();
    focused.value = true;
  }
};

const inputKeyword = (ev: Event) => {
  newKeyword.value = (ev.target as HTMLInputElement).value;
};

const keywordChange = () => {
  emit('update:modelValue', innerKeywords.value);
};

const addKeyword = (keyword: string) => {
  const trimmedKeyword = keyword.trim();
  if (
    trimmedKeyword
    && !innerKeywords.value.includes(trimmedKeyword)
  ) {
    innerKeywords.value.push(trimmedKeyword);
    return true;
  }
  return false;
};

const addNew = (e?: KeyboardEvent | FocusEvent) => {
  if (
    e
    && !props.addKeywordOnKeys.includes((e as KeyboardEvent).keyCode)
    && e.type !== 'blur'
  ) {
    return;
  }
  if (e) {
    e.stopPropagation();
    e.preventDefault();
  }
  let addSuccess = false;
  if (newKeyword.value.includes(',')) {
    newKeyword.value.split(',').forEach((item) => {
      if (addKeyword(item.trim())) {
        addSuccess = true;
      }
    });
  } else if (addKeyword(newKeyword.value.trim())) {
    addSuccess = true;
  }
  if (addSuccess) {
    keywordChange();
    newKeyword.value = '';
  }
};

const blur = (e: FocusEvent) => {
  focused.value = false;
  addNew(e);
};

const focus = () => {
  focused.value = true;
};

const remove = (index: number) => {
  innerKeywords.value.splice(index, 1);
  keywordChange();
};

const removeLastKeyword = () => {
  if (newKeyword.value) {
    return;
  }
  innerKeywords.value.pop();
  keywordChange();
};
</script>

<style lang="scss">
.app-keywords-input {
  @apply w-full;

  .el-keywords-input-wrapper {
    @apply relative text-sm bg-white shadow-none border border-solid border-gray-300 rounded-md pr-2 pl-1 flex items-center flex-wrap;
    background-image: none;
    box-sizing: content-box;
    outline: none;
    transition: border-color 0.2s
      cubic-bezier(0.645, 0.045, 0.355, 1);
    min-height: 38px;

    .c-tag {
      @apply m-1 mr-0 text-tiny;
    }

    &.is-focus {
      @apply border-gray-500;
    }

    &:hover:not(:focus):not(.is-focus) {
      @apply border-gray-400;
    }
  }

  .el-keywords-input {
    @apply bg-transparent border-none pl-0 ml-2 h-full text-gray-900;
    font-size: inherit;
    outline: none;
    width: 400px;
    min-height: 24px;
  }

  &.is-focus {
    border: 1px solid #0068bd;
  }

  .c-tag.c-tag--secondary {
    @apply text-black;
  }

  .el-keywords-input-list {
    @apply flex items-center flex-wrap h-full;
  }

  .helper-copy {
    @apply text-xs text-gray-400;
  }
}
</style>
