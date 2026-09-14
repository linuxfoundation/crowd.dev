# SQL & Migration Review Checklist

Review standards for database queries in `services/libs/data-access-layer/` and migrations in `backend/src/database/migrations/`.

---

## Flyway Migrations

### 1. Migrations are append-only (CRITICAL)

Never modify an existing migration file that has been applied. Create a new migration to alter or fix a previous one.

**Violation:** Editing `V1234__create_members_table.sql` after it has been applied.

**Fix:** Create `V1235__alter_members_add_column.sql` with the corrective change.

---

### 2. Migration filename format (SHOULD FIX)

Migration files must follow: `V{epoch}__{description}.sql` and `U{epoch}__{description}.sql` (undo). The version must be unique and greater than all existing versions.

---

### 3. Migrations must be safe for production (CRITICAL)

Avoid:
- `DROP TABLE` without verifying data is no longer needed
- `ALTER TABLE ... NOT NULL` without a default or two-step migration (add nullable first, backfill, then add constraint)
- Renaming columns without updating all code references first
- Large table operations without considering lock impact

---

## Data Access Layer Queries

### 4. Parameterized queries only — no string interpolation (CRITICAL)

All queries must use parameterized placeholders (`$1`, `$2`, etc.). Never interpolate user input directly into SQL.

**Violation:**
```ts
await queryExecutor.query(`SELECT * FROM members WHERE email = '${email}'`)
```

**Fix:**
```ts
await queryExecutor.query('SELECT * FROM members WHERE email = $1', [email])
```

---

### 5. Placeholder count matches bind values (CRITICAL)

Every `$N` placeholder must have a corresponding value in the binds array, in correct order. Mismatch causes runtime errors.

---

### 6. Index awareness (SHOULD FIX)

Before writing a query, verify the table has an index on the WHERE clause columns. Flag queries that will cause full table scans on large tables (`members`, `activities`, `organizations`).

Common indexed columns: `id`, `tenantId`, `platform`, `sourceId`, `memberId`, `organizationId`, `timestamp`, `deletedAt`.

---

### 7. Blast radius check for modified DAL functions (SHOULD FIX)

If a shared DAL function in `services/libs/data-access-layer/src/` is modified, check all callers. Use `grep -r "functionName" services/ backend/` to find them.

---

### 8. Soft-delete awareness (SHOULD FIX)

Many tables use `deletedAt` for soft deletes. Queries that don't filter on `deletedAt IS NULL` may return deleted records. Verify the intended behavior.
