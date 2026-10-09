import { ref, computed, type Ref, type ComputedRef } from 'vue';

export default function elementChangeDetector(element: Ref<unknown>): {
  temporaryElement: Ref<string>;
  elementSnapshot: () => void;
  hasElementChanged: ComputedRef<boolean>;
} {
  const temporaryElement = ref('');

  function elementSnapshot(): void {
    temporaryElement.value = JSON.stringify(element.value);
  }

  const hasElementChanged = computed(
    () => temporaryElement.value !== JSON.stringify(element.value),
  );

  return {
    temporaryElement,
    elementSnapshot,
    hasElementChanged,
  };
}
