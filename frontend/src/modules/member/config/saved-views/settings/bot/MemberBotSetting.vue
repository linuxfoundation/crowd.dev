<template>
  <article>
    <p class="text-xs font-semibold leading-5 mb-3">
      Bots visibility
    </p>
    <el-radio-group v-model="value" :disabled="props.settings?.teamMember === 'filter'">
      <el-radio :label="IncludeEnum.EXCLUDE" class="!h-5">
        <span class="text-xs">Exclude bots</span>
      </el-radio>
      <el-radio :label="IncludeEnum.INCLUDE" class="!h-5">
        <span class="text-xs">Include bots</span>
      </el-radio>
    </el-radio-group>
  </article>
</template>

<script setup lang="ts">
import { IncludeEnum } from '@/modules/member/config/saved-views/settings/common/types/IncludeEnum';
import { computed, ref, watch } from 'vue';

const props = defineProps<{
    modelValue: IncludeEnum | '',
    settings: Record<string, string>,
  }>();

const emit = defineEmits<{(e: 'update:modelValue', value: IncludeEnum | ''): void }>();

const value = computed<IncludeEnum | ''>({
  get() {
    return props.modelValue;
  },
  set(val: IncludeEnum | '') {
    emit('update:modelValue', val);
  },
});

const previousSettings = ref<Record<string, string> | null>(null);

watch(() => props.settings, (current) => {
  if (current?.teamMember === 'filter') {
    if (!previousSettings.value || previousSettings.value.teamMember !== 'filter') {
      value.value = '';
    }
  }
  if (previousSettings.value?.teamMember === 'filter' && current?.teamMember !== 'filter') {
    value.value = IncludeEnum.EXCLUDE;
  }
  previousSettings.value = { ...current };
}, { deep: true, immediate: true });
</script>

<script lang="ts">
export default {
  name: 'LfMemberTeamMemberSetting',
};
</script>
