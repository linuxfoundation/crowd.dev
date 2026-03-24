<template>
  <div class="app-keywords-input">
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
        :placeholder="placeholder"
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

<script>
import LfTag from '@/ui-kit/tag/Tag.vue';

export default {
  name: 'AppKeywordsInput',
  components: {
    LfTag,
  },
  props: {
    modelValue: {
      type: Array,
      default: () => [],
    },
    addKeywordOnKeys: {
      type: Array,
      default: () => [13, 188, 9],
    },
    readOnly: {
      type: Boolean,
      default: false,
    },
    placeholder: {
      type: String,
      default: null,
    },
    isError: {
      type: Boolean,
      default: false,
    },
    showHint: {
      type: Boolean,
      default: false,
    },
    hintText: {
      type: String,
      default: 'Press ENTER or comma (,) to separate keywords.',
    },

  },
  emits: ['update:modelValue'],
  data() {
    return {
      newKeyword: '',
      innerKeywords: [...this.modelValue],
      focused: false,
    };
  },
  watch: {
    modelValue() {
      this.innerKeywords = [...this.modelValue];
    },
  },
  methods: {
    focusKeywordInput() {
      if (
        !(this.readOnly
        || !this.$el.querySelector('.el-keywords-input'))) {
        this.$el.querySelector('.el-keywords-input').focus();
        this.focused = true;
      }
    },
    inputKeyword(ev) {
      this.newKeyword = ev.target.value;
    },
    blur(e) {
      this.focused = false;
      this.addNew(e);
    },
    focus() {
      this.focused = true;
    },
    addNew(e) {
      if (
        e
        && !this.addKeywordOnKeys.includes(e.keyCode)
        && e.type !== 'blur'
      ) {
        return;
      }
      if (e) {
        e.stopPropagation();
        e.preventDefault();
      }
      let addSuccess = false;
      if (this.newKeyword.includes(',')) {
        this.newKeyword.split(',').forEach((item) => {
          if (this.addKeyword(item.trim())) {
            addSuccess = true;
          }
        });
      } else if (this.addKeyword(this.newKeyword.trim())) {
        addSuccess = true;
      }
      if (addSuccess) {
        this.keywordChange();
        this.newKeyword = '';
      }
    },
    addKeyword(keyword) {
      const trimmedKeyword = keyword.trim();
      if (
        trimmedKeyword
        && !this.innerKeywords.includes(trimmedKeyword)
      ) {
        this.innerKeywords.push(trimmedKeyword);
        return true;
      }
      return false;
    },
    remove(index) {
      this.innerKeywords.splice(index, 1);
      this.keywordChange();
    },
    removeLastKeyword() {
      if (this.newKeyword) {
        return;
      }
      this.innerKeywords.pop();
      this.keywordChange();
    },
    keywordChange() {
      this.$emit('update:modelValue', this.innerKeywords);
    },
  },
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
