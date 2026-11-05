<template>
  <article>
    <p class="text-xs font-semibold leading-5 mb-3">
      Team members visibility
    </p>
    <el-radio-group v-model="value">
      <el-radio :label="IncludeEnum.EXCLUDE" class="!h-5">
        <span class="text-xs">Exclude team members</span>
      </el-radio>
      <el-radio :label="IncludeEnum.INCLUDE" class="!h-5">
        <span class="text-xs">Include team members</span>
      </el-radio>
      <el-radio :label="IncludeEnum.FILTER" class="!h-5">
        <span class="text-xs">Team members only</span>
      </el-radio>
    </el-radio-group>
  </article>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { IncludeEnum } from '@/modules/member/config/saved-views/settings/common/types/IncludeEnum';

const props = defineProps<{
  modelValue: IncludeEnum,
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: IncludeEnum): void }>();

const value = computed<IncludeEnum>({
  get() {
    return props.modelValue;
  },
  set(val: IncludeEnum) {
    emit('update:modelValue', val);
  },
});
</script>

<script lang="ts">
export default {
  name: 'LfMemberTeamMemberSetting',
};
</script>
