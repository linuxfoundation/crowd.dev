# Automatic Projects Discovery Worker

Temporal worker that discovers open-source projects from external data sources and writes them to the `projectCatalog` table.

## Architecture

### Source abstraction

Every data source implements the `IDiscoverySource` interface (`src/sources/types.ts`):

| Method                        | Purpose                                                                     |
| ----------------------------- | --------------------------------------------------------------------------- |
| `listAvailableDatasets()`     | Returns available dataset snapshots, sorted newest-first                    |
| `fetchDatasetStream(dataset)` | Returns a readable stream for the dataset (e.g. HTTP response)              |
| `parseRow(rawRow)`            | Converts a raw CSV/JSON row into a `IDiscoverySourceRow`, or `null` to skip |

Sources are registered in `src/sources/registry.ts`. `CROWD_DISCOVERY_SOURCES` (comma-separated
source names) restricts which ones run; unset enables all of them.

**To add a new source:** create a class implementing `IDiscoverySource`, then add one line to the registry.

### Current sources

| Name                   | Folder                              | Description                                                                                           |
| ---------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `insights-discussions` | `src/sources/insights-discussions/` | Repo URLs mentioned in `linuxfoundation/insights` GitHub Discussions (`project-onboardings` category) |
| `lf-criticality-score` | `src/sources/lf-criticality-score/` | LF Criticality Score API, paginated, ordered by score descending                                      |

### Workflow

```
discoverProjects({ mode: 'incremental' | 'full' })
  │
  ├─ Activity: listDatasets(sourceName)
  │   → returns dataset descriptors sorted newest-first
  │
  ├─ Selection: incremental → latest only, full → all datasets
  │
  └─ For each dataset:
      └─ Activity: processDataset(sourceName, dataset)
          → stream rows → parseRow → keep up to DISCOVERY_NEW_PROJECTS_LIMIT rows
            whose repoUrl isn't already in projectCatalog → bulkInsertProjectCatalog
```

Each source is capped independently at `CROWD_DISCOVERY_NEW_PROJECTS_LIMIT` (default 20) _new_
projects per `processDataset` call — rows already present in `projectCatalog` are still fetched
from the source but don't count against the cap, so every run brings in genuinely new candidates.
Sources are processed in registry order (`insights-discussions`, then `lf-criticality-score`);
once a source hits its limit, `processDataset` stops consuming its stream early.

In `full` mode the cap applies per dataset, not per source: a source with N historical dataset
snapshots can add up to `N × CROWD_DISCOVERY_NEW_PROJECTS_LIMIT` new rows in a single `full` run,
since each dataset is processed by its own `processDataset` call. This is intentional for an
initial backfill; `incremental` (the daily schedule) only ever processes the latest dataset, so
the cap is a true per-source-per-run limit there.

### Timeouts

| Activity           | startToCloseTimeout | retries | notes                 |
| ------------------ | ------------------- | ------- | --------------------- |
| `listDatasets`     | 2 min               | 3       |                       |
| `processDataset`   | 90 min              | 3       | heartbeat every 5 min |
| Workflow execution | 5 hours             | 3       | set on the schedule   |

### Schedule

Runs daily at midnight via Temporal cron (`0 0 * * *`).

## File structure

```
src/
├── main.ts                          # Service bootstrap (postgres enabled)
├── config.ts                        # Shared env config (parseEnvInt, DISCOVERY_NEW_PROJECTS_LIMIT)
├── activities.ts                    # Barrel re-export
├── workflows.ts                     # Barrel re-export
├── activities/
│   └── activities.ts                # listSources, listDatasets, processDataset
├── workflows/
│   └── discoverProjects.ts          # Orchestration with mode selection
├── schedules/
│   └── scheduleProjectsDiscovery.ts # Temporal cron schedule
└── sources/
    ├── types.ts                     # IDiscoverySource, IDatasetDescriptor
    ├── registry.ts                  # Source list + CROWD_DISCOVERY_SOURCES gate
    ├── insights-discussions/
    │   └── source.ts                # IDiscoverySource implementation
    └── lf-criticality-score/
        └── source.ts                # IDiscoverySource implementation
```
