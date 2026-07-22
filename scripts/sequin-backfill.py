#!/usr/bin/env python3
"""Trigger date-based Sequin backfills for all sinks in a database.

Interactive flow:
  1. Pick a Sequin environment (from `sequin` CLI contexts in ~/.sequin/contexts)
  2. Pick a database
  3. Enter a start date (UTC) — backfills replay rows with sort column >= date
  4. Review the table checklist (all selected by default, deselect any)
  5. Confirm — one backfill is created per (sink, table) pair

Reads (databases, sinks) go through the Sequin Management API using the CLI
context's host + API token. Backfill creation goes through `bin/sequin rpc`
on the running node, because the HTTP API in v0.13.x only supports full-table
backfills — a date-bounded start position requires
`Sequin.Consumers.create_backfills_for_form/3` with a partial-backfill config.

Non-interactive example:
  scripts/sequin-backfill.py --context lfx-prod --database CM2 \
      --date 2026-07-01 --tables public.members,public.organizations --yes

Only remote environments with an exec mapping in CONTEXT_EXEC (or via
SEQUIN_RPC_EXEC) are supported.

Overrides:
  SEQUIN_RPC_EXEC     command prefix that reaches the sequin container
                      (e.g. "kubectl -n default exec -i deploy/sequin --")
  SEQUIN_RELEASE_BIN  path of the release script inside the container
                      (default: /home/app/prod/rel/sequin/bin/sequin)
"""

import argparse
import json
import os
import re
import shlex
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

CONTEXTS_DIR = Path.home() / ".sequin" / "contexts"
SORT_COLUMN_CANDIDATES = [
    "updatedAt", "updated_at",
    "lastSyncedAt", "last_synced_at",
    "snapshotAt", "snapshot_at",
    "verifiedAt", "verified_at",
]
RELEASE_BIN = os.environ.get("SEQUIN_RELEASE_BIN", "/home/app/prod/rel/sequin/bin/sequin")

# How to reach `bin/sequin rpc` for each CLI context. Contexts not listed here
# require SEQUIN_RPC_EXEC.
CONTEXT_EXEC = {
    "lfx-prod": ["kubectl", "--context", "cm-prod-oracle", "-n", "default",
                 "exec", "-i", "deploy/sequin", "--"],
}

ELIXIR_TEMPLATE = r'''
payload = Jason.decode!("__PAYLOAD__")
sort_cols = payload["sort_columns"]
date = payload["date"]

out =
  Enum.map(payload["sinks"], fn %{"id" => sid, "tables" => trefs} ->
    case Sequin.Repo.get(Sequin.Consumers.SinkConsumer, sid) do
      nil ->
        %{sink: sid, created: [], failed: ["sink consumer not found"], skipped: []}

      c ->
        c = Sequin.Repo.preload(c, :postgres_database)

        {cfgs, skipped, oid_map} =
          Enum.reduce(trefs, {[], [], %{}}, fn tref, {cfgs, skipped, oid_map} ->
            [schema, tname] = String.split(tref, ".", parts: 2)
            table = Enum.find(c.postgres_database.tables, fn t -> t.name == tname and t.schema == schema end)
            col =
              table &&
                Enum.find_value(sort_cols, fn name ->
                  Enum.find(table.columns, fn col -> col.name == name end)
                end)

            cond do
              is_nil(table) ->
                {cfgs, [%{table: tref, reason: "table not found in database"} | skipped], oid_map}

              is_nil(col) ->
                reason = "no sort column found (tried: " <> Enum.join(sort_cols, ", ") <> ")"
                {cfgs, [%{table: tref, reason: reason} | skipped], oid_map}

              true ->
                cfg = %{
                  "oid" => table.oid,
                  "type" => "partial",
                  "sortColumnAttnum" => col.attnum,
                  "initialMinCursor" => date
                }

                {[cfg | cfgs], skipped, Map.put(oid_map, table.oid, %{table: tref, sort_column: col.name})}
            end
          end)

        res =
          if cfgs == [] do
            %{}
          else
            Sequin.Consumers.create_backfills_for_form(c.account_id, c, cfgs)
          end

        created =
          res
          |> Map.get(:created, [])
          |> Enum.map(fn b ->
            info = Map.get(oid_map, b.table_oid, %{})
            %{id: b.id, table: info[:table], sort_column: info[:sort_column]}
          end)

        failed =
          res
          |> Map.get(:failed, [])
          |> Enum.map(fn cs ->
            Enum.map_join(cs.errors, "; ", fn {field, {msg, _}} -> "#{field}: #{msg}" end)
          end)

        if created != [] do
          Sequin.Runtime.Supervisor.maybe_start_table_readers(
            Sequin.Repo.preload(c, :active_backfills, force: true)
          )
        end

        %{sink: c.name, created: created, failed: failed, skipped: skipped}
    end
  end)

IO.puts("SEQUIN_BACKFILL_RESULT:" <> Jason.encode!(out))
'''


def die(msg, code=1):
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(code)


# ---------------------------------------------------------------- CLI contexts

def load_contexts():
    if not CONTEXTS_DIR.is_dir():
        die(f"no sequin CLI contexts found in {CONTEXTS_DIR} (run `sequin context add` first)")
    contexts = {}
    for path in sorted(CONTEXTS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            contexts[data["name"]] = data
        except (json.JSONDecodeError, KeyError):
            print(f"warning: skipping unreadable context file {path}", file=sys.stderr)
    if not contexts:
        die(f"no usable contexts in {CONTEXTS_DIR}")
    return contexts


# ------------------------------------------------------------- Management API

class SequinApi:
    def __init__(self, ctx):
        self.token = ctx["api_token"]
        self.host = ctx["hostname"]
        self.base = None  # resolved on first request

    def _try(self, base, path):
        req = urllib.request.Request(
            f"{base}{path}", headers={"Authorization": f"Bearer {self.token}"}
        )
        ssl_ctx = ssl.create_default_context()
        return urllib.request.urlopen(req, timeout=15, context=ssl_ctx)

    def get(self, path):
        if self.base:
            with self._try(self.base, path) as resp:
                return json.load(resp)
        last_err = None
        for scheme in ("https", "http"):
            base = f"{scheme}://{self.host}"
            try:
                with self._try(base, path) as resp:
                    data = json.load(resp)
                    self.base = base
                    return data
            except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
                last_err = e
        die(f"cannot reach Sequin API at {self.host}: {last_err}")

    def databases(self):
        for path in ("/api/postgres_databases", "/api/databases"):
            try:
                return self.get(path)["data"]
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    raise
        die("no database listing endpoint on this Sequin version")

    def sinks(self):
        try:
            return self.get("/api/sinks")["data"]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                die("this Sequin version has no /api/sinks endpoint — upgrade required")
            raise


# ------------------------------------------------------------------ prompting

def choose(title, labels):
    print(f"\n{title}")
    for i, label in enumerate(labels, 1):
        print(f"  {i}. {label}")
    while True:
        raw = input("> ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(labels):
            return int(raw) - 1
        print(f"enter a number between 1 and {len(labels)}")


def parse_toggles(raw, count):
    """'1 3-5,8' -> indices; None if malformed."""
    indices = set()
    for part in re.split(r"[,\s]+", raw.strip()):
        if not part:
            continue
        m = re.fullmatch(r"(\d+)(?:-(\d+))?", part)
        if not m:
            return None
        lo = int(m.group(1))
        hi = int(m.group(2) or lo)
        if not (1 <= lo <= hi <= count):
            return None
        indices.update(range(lo - 1, hi))
    return indices


def choose_tables(tables, sinks_by_table):
    selected = [True] * len(tables)
    while True:
        print("\nTables to backfill (all sinks on a deselected table are skipped):")
        for i, table in enumerate(tables, 1):
            mark = "x" if selected[i - 1] else " "
            sinks = ", ".join(s["name"] for s in sinks_by_table[table])
            print(f"  [{mark}] {i:2}. {table}  ({sinks})")
        print("toggle: numbers/ranges (e.g. '2 4-6') · a=all & confirm · n=none · Enter=confirm · q=abort")
        raw = input("> ").strip().lower()
        if raw == "":
            if any(selected):
                return [t for t, keep in zip(tables, selected) if keep]
            print("nothing selected — select at least one table or 'q' to abort")
        elif raw == "a":
            return list(tables)
        elif raw == "n":
            selected = [False] * len(tables)
        elif raw == "q":
            die("aborted", 130)
        else:
            toggles = parse_toggles(raw, len(tables))
            if toggles is None:
                print("unrecognized input")
            else:
                for i in toggles:
                    selected[i] = not selected[i]


def normalize_date(raw):
    """Accept YYYY-MM-DD[ HH:MM[:SS]] or ISO-8601; return UTC ISO string."""
    raw = raw.strip()
    candidate = raw.replace(" ", "T", 1)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
        candidate += "T00:00:00"
    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}", candidate):
        candidate += ":00"
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------------------------------------------ rpc layer

def resolve_exec(ctx):
    override = os.environ.get("SEQUIN_RPC_EXEC")
    if override:
        return shlex.split(override)
    if ctx["name"] in CONTEXT_EXEC:
        return CONTEXT_EXEC[ctx["name"]]
    die(f"no exec mapping for context '{ctx['name']}' — set SEQUIN_RPC_EXEC")


def build_elixir(plan, date, sort_columns):
    payload = json.dumps({
        "date": date,
        "sort_columns": sort_columns,
        "sinks": [{"id": s["id"], "tables": tables} for s, tables in plan],
    })
    escaped = (
        payload.replace("\\", "\\\\").replace('"', '\\"').replace("#{", "\\#{")
    )
    return ELIXIR_TEMPLATE.replace("__PAYLOAD__", escaped)


def run_rpc(exec_prefix, code):
    cmd = exec_prefix + [RELEASE_BIN, "rpc", code]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    marker = "SEQUIN_BACKFILL_RESULT:"
    for line in proc.stdout.splitlines():
        if line.startswith(marker):
            return json.loads(line[len(marker):])
    print(proc.stdout, file=sys.stderr)
    print(proc.stderr, file=sys.stderr)
    die(f"rpc did not return a result (exit {proc.returncode})")


# ----------------------------------------------------------------------- main

def main():
    parser = argparse.ArgumentParser(description="Trigger date-based Sequin backfills")
    parser.add_argument("--context", help="sequin CLI context name")
    parser.add_argument("--database", help="Sequin database name")
    parser.add_argument("--date", help="start date, UTC (YYYY-MM-DD or ISO-8601)")
    parser.add_argument("--tables", help="comma-separated schema.table list (default: all)")
    parser.add_argument("--sort-column",
                        help="force a specific sort column; default auto-detects "
                             f"per table ({', '.join(SORT_COLUMN_CANDIDATES)})")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the plan and generated code without executing")
    parser.add_argument("--yes", action="store_true", help="skip confirmation")
    args = parser.parse_args()

    contexts = load_contexts()
    if not os.environ.get("SEQUIN_RPC_EXEC"):
        contexts = {n: c for n, c in contexts.items() if n in CONTEXT_EXEC}
        if not contexts:
            die("no contexts with an exec mapping — add one to CONTEXT_EXEC or set SEQUIN_RPC_EXEC")
    if args.context:
        if args.context not in contexts:
            die(f"unknown or unsupported context '{args.context}' (available: {', '.join(contexts)})")
        ctx = contexts[args.context]
    elif len(contexts) == 1:
        ctx = next(iter(contexts.values()))
        print(f"Using context '{ctx['name']}' ({ctx['hostname']})")
    else:
        names = list(contexts)
        labels = [f"{n}  ({contexts[n]['hostname']})" for n in names]
        ctx = contexts[names[choose("Select Sequin environment:", labels)]]

    api = SequinApi(ctx)
    databases = api.databases()
    if not databases:
        die("no databases found in this Sequin instance")
    if args.database:
        db = next((d for d in databases if d["name"] == args.database), None)
        if not db:
            die(f"unknown database '{args.database}' "
                f"(available: {', '.join(d['name'] for d in databases)})")
    else:
        db = databases[choose("Select database:", [d["name"] for d in databases])]

    all_sinks = api.sinks()
    sinks = [s for s in all_sinks if s.get("database") == db["name"]]
    if not sinks:
        die(f"no sinks found for database '{db['name']}'")
    for sink in sinks:
        if not (sink.get("source") or {}).get("include_tables"):
            print(f"warning: sink '{sink['name']}' has no include_tables "
                  f"(schema-wide source) — skipping it", file=sys.stderr)
    sinks = [s for s in sinks if (s.get("source") or {}).get("include_tables")]
    if not sinks:
        die("no sinks with explicit source tables to backfill")

    sinks_by_table = {}
    for sink in sinks:
        for table in sink["source"]["include_tables"]:
            sinks_by_table.setdefault(table, []).append(sink)
    tables = sorted(sinks_by_table)

    if args.date:
        date = normalize_date(args.date)
        if not date:
            die(f"cannot parse date '{args.date}'")
    else:
        while True:
            date = normalize_date(input("\nBackfill start date, UTC (YYYY-MM-DD or ISO-8601): "))
            if date:
                break
            print("cannot parse that date, try again")

    if args.tables:
        wanted = [t.strip() for t in args.tables.split(",") if t.strip()]
        wanted = [t if "." in t else f"public.{t}" for t in wanted]
        unknown = [t for t in wanted if t not in sinks_by_table]
        if unknown:
            die(f"tables not covered by any sink in '{db['name']}': {', '.join(unknown)}")
        selected_tables = wanted
    elif sys.stdin.isatty():
        selected_tables = choose_tables(tables, sinks_by_table)
    else:
        selected_tables = tables

    # plan: one backfill per (sink, table-of-that-sink-still-selected)
    plan = []
    for sink in sinks:
        sink_tables = [t for t in sink["source"]["include_tables"] if t in selected_tables]
        if sink_tables:
            plan.append((sink, sink_tables))
    if not plan:
        die("selection matches no sinks — nothing to do")

    sort_columns = [args.sort_column] if args.sort_column else SORT_COLUMN_CANDIDATES
    sort_label = args.sort_column or f"auto ({' > '.join(SORT_COLUMN_CANDIDATES)})"
    print(f"\nPlan — backfill from {date} (sort column: {sort_label}) "
          f"on '{db['name']}' via context '{ctx['name']}':")
    for sink, sink_tables in plan:
        status = sink.get("status", "?")
        active = len(sink.get("active_backfills") or [])
        note = f" [status: {status}]" if status != "active" else ""
        note += f" [!! {active} active backfill(s) already]" if active else ""
        print(f"  {sink['name']}{note}: {', '.join(sink_tables)}")
    total = sum(len(t) for _, t in plan)
    print(f"  -> {total} backfill(s) across {len(plan)} sink(s)")

    code = build_elixir(plan, date, sort_columns)
    if args.dry_run:
        print("\n--- dry run: generated Elixir (executed via `bin/sequin rpc`) ---")
        print(code)
        return

    exec_prefix = resolve_exec(ctx)
    if not args.yes:
        prod = "prod" in ctx["name"] or "production" in ctx["hostname"]
        if prod:
            print("\n*** PRODUCTION environment ***")
        answer = input(f"\nType 'yes' to create {total} backfill(s): ").strip().lower()
        if answer != "yes":
            die("aborted", 130)

    print("\nTriggering backfills via rpc...")
    results = run_rpc(exec_prefix, code)

    print()
    failures = 0
    for res in results:
        for entry in res.get("created", []):
            print(f"  OK   {res['sink']} / {entry['table']}  "
                  f"(sort column: {entry.get('sort_column')}, backfill {entry['id']})")
        for msg in res.get("failed", []):
            failures += 1
            print(f"  FAIL {res['sink']}: {msg}")
        for skip in res.get("skipped", []):
            failures += 1
            print(f"  SKIP {res['sink']} / {skip['table']}: {skip['reason']}")
    created_total = sum(len(r.get("created", [])) for r in results)
    print(f"\n{created_total}/{total} backfill(s) created."
          + (" Check failures above." if failures else ""))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        sys.exit(130)
