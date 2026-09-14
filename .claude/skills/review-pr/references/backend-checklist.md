# Backend Review Checklist

Express.js / pg-promise API review standards for the CDP repo (`backend/`).

---

## 1. New Sequelize usage (CRITICAL)

All new database code must use `queryExecutor` from `@crowd/data-access-layer`, not Sequelize. Sequelize is legacy and limited to existing usage in `backend/src/database/repositories/` and `backend/src/services/`.

**Violation:**
```ts
import { Sequelize } from 'sequelize'
const result = await Model.findAll({ where: { id } })
```

**Fix:**
```ts
// Use an existing DAL function from services/libs/data-access-layer/src/
// or add a new one using queryExecutor
```

Do **not** flag existing Sequelize in `backend/src/database/repositories/` or `backend/src/services/` — those are legacy files being migrated incrementally.

---

## 2. New public endpoints missing Zod + validateOrThrow (CRITICAL)

All new public API endpoints must validate input with a Zod schema using `validateOrThrow`.

**Violation:**
```ts
router.post('/members', async (req, res) => {
  const { name, email } = req.body // no validation
})
```

**Fix:**
```ts
import { z } from 'zod'
import { validateOrThrow } from '@crowd/common'

const schema = z.object({ name: z.string(), email: z.string().email() })

router.post('/members', async (req, res) => {
  const body = validateOrThrow(schema, req.body)
})
```

---

## 3. New multi-tenant logic (CRITICAL)

Multi-tenancy is being phased out. New code must use `DEFAULT_TENANT_ID` from `@crowd/common` rather than introducing new multi-tenant logic.

**Fix:**
```ts
import { DEFAULT_TENANT_ID } from '@crowd/common'
```

---

## 4. New class-based services or repositories (SHOULD FIX)

New code should use plain functions, not class-based patterns.

**Violation:**
```ts
export class MemberService {
  async findById(id: string) { ... }
}
```

**Fix:**
```ts
export async function findMemberById(id: string) { ... }
```

---

## 5. DAL function added without checking for existing equivalents (SHOULD FIX)

Before adding a new function to `services/libs/data-access-layer/src/`, verify no equivalent already exists. Flag any new DAL function that appears to duplicate an existing one.

---

## 6. `any` types in new code (SHOULD FIX)

Avoid `any`. Use proper types, `unknown` with narrowing, or generics.

---

## 7. No secrets hardcoded (CRITICAL)

API keys, tokens, and credentials must come from environment variables, never hardcoded.

---

## 8. Auth0 vs legacy JWT (SHOULD FIX)

New auth code must use Auth0 patterns. Do not introduce new legacy JWT patterns.

---

## Known false positives — do NOT flag

- Sequelize usage in `backend/src/database/repositories/` or `backend/src/services/` — legacy files
- `DEFAULT_TENANT_ID` usage — this is the correct pattern
- Zod usage outside `validateOrThrow` (e.g. internal validation) — not a violation
