# ADR-0029: TypeScript project references

**Date**: 2026-09-22
**Status**: accepted
**Deciders**: Yeganathan S

## Context

Typechecking was `tsc --noEmit` per package, and CI also typechecked every dependent of a changed library. Packages import each other as TypeScript source (`main` points at `src/index.ts`, and deep imports such as `@crowd/data-access-layer/src/...` are normal), so the shared source was rechecked in every dependent. Most of the CI time was that repeated work, not the application code in the package that changed.

## Decision

Typecheck services and the backend with TypeScript project references. `pnpm tsc-check` runs `tsc -b` on the root `tsconfig.json`, which references every project that should be built. Shared services compiler options live in `services/base.tsconfig.json`. A package config extends that file and references the `@crowd` packages it imports.

`tsc -b` builds each project once and writes `.d.ts` and `.tsbuildinfo` under that project's `dist/`. Dependents use the `.d.ts`. Those files are gitignored. A later `tsc -b` on a machine that still has them skips projects whose outputs are up to date. CI checks out a clean tree, so it typechecks the solution from scratch. Services still run from TypeScript source through `tsx`.

`typescript` is a root devDependency, taken from the catalog, because `pnpm tsc-check` runs `tsc` from the repo root. A package depends on TypeScript only when one of its own scripts runs `tsc`: the backend build, the archived-repositories cronjob, and `.github/actions/node`.

A few projects stay outside the shared services config. Data-access-layer tests import `@crowd/test-kit`, and test-kit imports the data-access layer, so those tests live in `services/libs/data-access-layer/tsconfig.test.json`, which the root config references. `backend/tsconfig.json` still emits JavaScript for sequelize-cli. `backend/tsconfig.check.json` extends it and only adds the project-reference emit settings. Delete that file once sequelize-cli is gone. The archived-repositories cronjob and `.github/actions/node` also emit JavaScript with `tsc` and are not part of `tsc -b`.

## Alternatives Considered

### Alternative 1: Keep per-package `tsc --noEmit`

- **Pros**: Nothing is generated. A new package is checked as soon as it has a `tsc-check` script.
- **Cons**: Dependents recheck shared source, which is the cost this decision removes.
- **Why not**: That is the workflow being replaced.

### Alternative 2: One `tsc --noEmit` over the whole repo

- **Pros**: One command, and no list of projects to maintain.
- **Cons**: Backend and services do not share compiler options, and library source would still be pulled into one program.
- **Why not**: The repo is many packages with different settings. One program does not match that.

### Alternative 3: Generate the root `references` with a script

- **Pros**: A new package is picked up without editing the root config by hand.
- **Cons**: The script would have to special-case the backend check config, the data-access-layer test config, and the packages that emit JavaScript.
- **Why not**: The root `tsconfig.json` is already that list. A generator would be a second copy of it.

## Consequences

### Positive

- Each package is typechecked once.
- A second local run can skip unchanged projects. CI typechecks the solution from scratch on a clean checkout.
- Declaration emit catches exported types that another package cannot name. `tsc --noEmit` did not.
- Services package configs no longer copy compiler options.

### Negative

- From a dependent, go to definition opens the generated `.d.ts`. Declaration maps are not turned on.

### Risks

- `tsc -b` only builds projects listed in the root `tsconfig.json`. A package that is missing from that list is not typechecked, and CI still passes. `CLAUDE.md` points agents at this when a package is added.
