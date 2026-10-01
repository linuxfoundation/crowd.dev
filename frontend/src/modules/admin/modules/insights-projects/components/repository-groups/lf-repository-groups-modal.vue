<template>
  <lf-modal v-model="isModalOpen" :teleport="false">
    <div class="pt-6 px-6 pb-8">
      <h5 class="text-h5 mb-4">
        {{ isEdit ? 'Edit' : 'Add' }} repository group
      </h5>
      <lf-field label-text="Group name" required class="mb-6">
        <lf-input
          v-model="form.name"
          :invalid="$v.name.$invalid && $v.name.$dirty"
          @blur="$v.name.$touch()"
          @change="$v.name.$touch()"
        />
        <lf-field-messages
          :validation="$v.name"
          :error-messages="{ required: 'This field is required' }"
        />
      </lf-field>

      <lf-field label-text="Repositories" required>
        <div>
          <el-select
            v-model="form.repositories"
            filterable
            multiple
            placeholder="Select option(s)"
            class="w-full"
            @change="$v.repositories.$touch()"
          >
            <el-option
              v-for="item of props.repositories"
              :key="item.url"
              :label="item.label"
              :value="item.url"
            />
          </el-select>
        </div>

        <lf-field-messages
          :validation="$v.repositories"
          :error-messages="{ required: 'This field is required' }"
        />
      </lf-field>
    </div>
    <div class="px-6 py-4.5 bg-gray-50 flex justify-end gap-4">
      <lf-button type="secondary-ghost" @click="isModalOpen = false">
        Cancel
      </lf-button>
      <lf-button type="primary" :disabled="$v.$invalid" @click="submit()">
        {{ isEdit ? 'Update' : 'Add ' }} repository group
      </lf-button>
    </div>
  </lf-modal>
</template>

<script lang="ts" setup>
import LfModal from '@/ui-kit/modal/Modal.vue';
import { computed, reactive } from 'vue';
import LfButton from '@/ui-kit/button/Button.vue';
import LfField from '@/ui-kit/field/Field.vue';
import LfInput from '@/ui-kit/input/Input.vue';
import useVuelidate from '@vuelidate/core';
import { required } from '@vuelidate/validators';
import LfFieldMessages from '@/ui-kit/field-messages/FieldMessages.vue';

interface RepositoryGroup {
  name: string;
  repositories: string[];
}
interface Repository {
  url: string;
  label: string;
}

const props = defineProps<{
  modelValue: boolean;
  repositories: Repository[];
  repositoryGroup?: RepositoryGroup | null;
}>();

const emit = defineEmits<{(e: 'update:modelValue', value: boolean): void;
  (e: 'add', value: RepositoryGroup): void;
  (e: 'edit', value: RepositoryGroup): void;
}>();

const isModalOpen = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
});

const isEdit = computed(() => !!props.repositoryGroup);

const form = reactive<RepositoryGroup>({
  name: props.repositoryGroup?.name || '',
  repositories: props.repositoryGroup?.repositories || [],
});

const rules = {
  name: {
    required,
  },
  repositories: {
    required,
  },
};

const $v = useVuelidate(rules, form, {
  $stopPropagation: true,
});

const submit = () => {
  if ($v.value.$invalid) {
    $v.value.$touch();
    return;
  }
  if (isEdit.value) {
    emit('edit', { ...form });
  } else {
    emit('add', { ...form });
  }
  isModalOpen.value = false;
};
</script>

<script lang="ts">
export default {
  name: 'LfRepositoryGroupsModal',
};
</script>
