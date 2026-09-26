import {
  ref, computed, type Ref, type ComputedRef,
} from 'vue';

export default function formChangeDetector<T extends object>(form: T): {
  temporaryForm: Ref<string>;
  formSnapshot: () => void;
  hasFormChanged: ComputedRef<boolean>;
} {
  const temporaryForm = ref('');

  function formSnapshot() {
    temporaryForm.value = JSON.stringify(form);
  }

  const hasFormChanged = computed(() => temporaryForm.value !== JSON.stringify(form));

  return {
    temporaryForm,
    formSnapshot,
    hasFormChanged,
  };
}
