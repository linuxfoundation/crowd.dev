import {
  ref, computed, type Ref, type ComputedRef,
} from 'vue';

export default function elementChangeDetector<T>(element: Ref<T>): {
  temporaryElement: Ref<string>;
  elementSnapshot: () => void;
  hasElementChanged: ComputedRef<boolean>;
} {
  const temporaryElement = ref('');

  function elementSnapshot() {
    temporaryElement.value = JSON.stringify(element.value);
  }

  const hasElementChanged = computed(() => (
    temporaryElement.value
      !== JSON.stringify(element.value)
  ));

  return {
    temporaryElement,
    elementSnapshot,
    hasElementChanged,
  };
}
