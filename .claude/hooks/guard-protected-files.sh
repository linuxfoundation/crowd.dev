#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to CDP.
# SPDX-License-Identifier: MIT
#
# Guard hook: warns when editing protected infrastructure files.
# Used by Claude Code PreToolUse hook on Edit and Write operations.
# Exit code 0 = allow (with warning message printed to stderr).

set -euo pipefail

# Read tool input from stdin (JSON with file_path field)
INPUT=$(cat)

# Extract file_path from the JSON input
FILE_PATH=$(echo "$INPUT" | grep -o '"file_path"[[:space:]]*:[[:space:]]*"[^"]*"' | head -1 | sed 's/.*"file_path"[[:space:]]*:[[:space:]]*"//;s/"$//' || true)

# If no file_path found, allow the operation
if [ -z "$FILE_PATH" ]; then
  exit 0
fi

# Normalize: strip leading ./ if present
FILE_PATH="${FILE_PATH#./}"

# Also handle absolute paths by stripping the repo root
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "")
if [ -n "$REPO_ROOT" ] && [[ "$FILE_PATH" == "$REPO_ROOT"/* ]]; then
  FILE_PATH="${FILE_PATH#$REPO_ROOT/}"
fi

# Helper: warn about a protected file (allows the edit to proceed)
warn() {
  local reason="$1"
  echo "" >&2
  echo "⚠ WARNING: This file is part of the project's core infrastructure." >&2
  echo "File: $FILE_PATH" >&2
  echo "Reason: $reason" >&2
  echo "Ensure this change is intentional and reviewed by a code owner." >&2
  echo "" >&2
  exit 0
}

# ── Data Access Layer ──────────────────────────────────────────────
if [[ "$FILE_PATH" == services/libs/data-access-layer/* ]]; then
  warn "Shared DAL — high blast radius. Check for existing functions before adding new ones (CLAUDE.md: 'duplicates are already a problem')."
fi

# ── Common / Shared Utilities ──────────────────────────────────────
if [[ "$FILE_PATH" == services/libs/common/* ]]; then
  warn "Shared utility library — changes affect all services and the backend."
fi

# ── Database Migrations ────────────────────────────────────────────
if [[ "$FILE_PATH" == backend/src/database/migrations/* ]]; then
  warn "Flyway migration — append-only. Never modify an existing migration that has been applied."
fi

# ── Dev CLI & Scaffold ─────────────────────────────────────────────
case "$FILE_PATH" in
  scripts/cli)
    warn "Dev CLI entrypoint — changes affect all contributors' local setup." ;;
  scripts/scaffold.yaml|scripts/scaffold.insights.yaml)
    warn "Docker scaffold config — changes affect how local services are started." ;;
esac

# ── Git Hooks & Commit Standards ──────────────────────────────────
if [[ "$FILE_PATH" == .husky/* ]]; then
  warn "Git hooks — changes affect pre-commit validation for all contributors."
fi

case "$FILE_PATH" in
  commitlint.config.js|commitlint.config.*)
    warn "Commitlint config — changes affect commit message validation for all contributors." ;;
esac

# ── CI / GitHub Actions ────────────────────────────────────────────
if [[ "$FILE_PATH" == .github/workflows/* ]] || [[ "$FILE_PATH" == .github/actions/* ]]; then
  warn "CI/CD pipeline — changes affect automated checks run on every PR."
fi

# ── Lint / Format / TypeScript Config ─────────────────────────────
case "$FILE_PATH" in
  tsconfig*.json|*/tsconfig*.json)
    warn "TypeScript configuration — changes affect compilation across the monorepo." ;;
  .eslintrc*|*/eslintrc*|eslint.config*|*/eslint.config*)
    warn "ESLint configuration — changes affect code quality rules for the project." ;;
  .prettierrc*|*/.prettierrc*)
    warn "Prettier configuration — changes affect code formatting standards." ;;
esac

# ── Package Files ──────────────────────────────────────────────────
if [[ "$FILE_PATH" == pnpm-lock.yaml ]]; then
  warn "Lock file — changes affect resolved dependency versions for all contributors."
fi
if [[ "$FILE_PATH" == package.json ]] || [[ "$FILE_PATH" == */package.json ]]; then
  warn "Package manifest — changes affect dependencies and scripts for this workspace."
fi

# ── Claude Config ─────────────────────────────────────────────────
case "$FILE_PATH" in
  CLAUDE.md)
    warn "Project instructions — changes affect AI assistant behavior for all users." ;;
  .claude/settings.json)
    warn "Claude Code settings — changes affect hooks, permissions, and plugins for all users." ;;
esac
if [[ "$FILE_PATH" == .claude/hooks/* ]]; then
  warn "Claude Code hook — changes affect automated pre-tool validations."
fi

# If none of the protected patterns matched, allow the operation
exit 0
