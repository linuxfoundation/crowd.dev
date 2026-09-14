# Frontend Review Checklist

Vue 3 / Vite frontend review standards for the CDP repo (`frontend/`).

---

## 1. Composition API with `<script setup>` (SHOULD FIX)

Use `<script setup>` with Composition API. No Options API components.

**Violation:**
```vue
<script>
export default {
  data() { return { count: 0 } }
}
</script>
```

**Fix:**
```vue
<script setup lang="ts">
const count = ref(0)
</script>
```

---

## 2. TanStack Vue Query for server state (SHOULD FIX)

Use `useQuery` / `useMutation` from TanStack Vue Query for data fetching. Do not use raw `axios` directly in `onMounted` for data that should be cached.

**Violation:**
```ts
const data = ref(null)
onMounted(async () => {
  data.value = await axios.get('/api/members')
})
```

**Fix:**
```ts
import { useQuery } from '@tanstack/vue-query'
const { data } = useQuery({
  queryKey: ['members'],
  queryFn: () => axios.get('/api/members').then(r => r.data),
})
```

---

## 3. Pinia for shared client state (SHOULD FIX)

Use Pinia stores for shared client-side state. Do not pass deeply nested props for state that should be in a store.

---

## 4. TypeScript — no `any` (SHOULD FIX)

Avoid `any`. Use proper types, `unknown` with narrowing, or generics.

---

## 5. Tailwind CSS conventions (SHOULD FIX)

- **Prefer `gap-*` over `space-y-*`** for vertical stacking
- No hard-coded hex color values in templates — use Tailwind color tokens

---

## 6. Reactive refs over non-reactive values (SHOULD FIX)

State that should trigger re-renders must use `ref()` or `computed()`.

**Violation:**
```ts
let isLoading = false // won't trigger re-render
```

**Fix:**
```ts
const isLoading = ref(false)
```

---

## 7. Element Plus usage patterns (NIT)

The project uses Element Plus (`el-*`). Follow existing component usage patterns — check how similar components are used elsewhere before introducing a new pattern.
