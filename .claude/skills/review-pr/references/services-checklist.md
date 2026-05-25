# Services Review Checklist

Review standards for microservices under `services/apps/` and shared libraries under `services/libs/`.

---

## Temporal Worker patterns (`services/apps/*/`)

### 1. Workflows must be deterministic (CRITICAL)

Temporal workflows must be fully deterministic. No direct I/O, no `Math.random()`, no `Date.now()`, no non-deterministic APIs inside workflow code. Move all I/O into Activities.

### 2. Activities should be idempotent (SHOULD FIX)

Temporal may retry activities on failure. Activities should be safe to run multiple times without unintended side effects.

### 3. No direct Kafka/Redis calls inside Temporal workflows (CRITICAL)

Kafka producers and Redis clients must only be called from Activities, not from Workflow code.

---

## Shared Libraries (`services/libs/*/`)

### 4. New DAL functions — check for existing equivalents first (SHOULD FIX)

Before adding a new function to `services/libs/data-access-layer/src/`, verify no equivalent exists.

### 5. `queryExecutor` from `@crowd/data-access-layer` (CRITICAL)

All new database queries must use `queryExecutor`. Do not add Sequelize or raw `pg` queries to service libraries.

### 6. Query performance awareness (SHOULD FIX)

Flag queries that clearly scan large tables without an appropriate WHERE clause using an indexed column.

### 7. Bunyan logger usage (SHOULD FIX)

Use the logger from `@crowd/logging`, not `console.log/error/warn`.

### 8. New class-based code (SHOULD FIX)

New service/worker code should use plain functions, not class-based patterns. Classes are legacy.

---

## Known false positives — do NOT flag

- Existing class-based workers that have not been refactored — only flag **new** classes
- `queryExecutor` usage in `services/libs/data-access-layer/` — correct pattern
- `DEFAULT_TENANT_ID` from `@crowd/common` — correct pattern
