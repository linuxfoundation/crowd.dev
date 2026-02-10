# Automatic Projects Discovery Worker

Temporal worker that discovers open-source projects from external data sources and writes them to the `projectCatalog` table.

## Architecture

### Source abstraction

Every data source implements the `IDiscoverySource` interface (`src/sources/types.ts`):

| Method | Purpose |
|--------|---------|
| `listAvailableDatasets()` | Returns available dataset snapshots, sorted newest-first |
| `fetchDatasetStream(dataset)` | Returns a readable stream for the dataset (e.g. HTTP response) |
| `parseRow(rawRow)` | Converts a raw CSV/JSON row into a `IDiscoverySourceRow`, or `null` to skip |

Sources are registered in `src/sources/registry.ts` as a simple name → factory map.

**To add a new source:** create a class implementing `IDiscoverySource`, then add one line to the registry.

### Current sources

| Name | Folder | Description |
|------|--------|-------------|
| `ossf-criticality-score` | `src/sources/ossf-criticality-score/` | OSSF Criticality Score snapshots from a public GCS bucket (~750K repos per snapshot) |

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
          → HTTP stream → csv-parse → batches of 5000 → bulkUpsertProjectCatalog
```

### Timeouts

| Activity | startToCloseTimeout | retries |
|----------|-------------------|---------|
| `listDatasets` | 2 min | 3 |
| `processDataset` | 30 min | 3 |
| Workflow execution | 2 hours | 3 |

### Schedule

Runs daily via Temporal cron. The cron expression can be overridden with the `CROWD_AUTOMATIC_PROJECTS_DISCOVERY_CRON` env var.

## File structure

```
src/
├── main.ts                          # Service bootstrap (postgres enabled)
├── activities.ts                    # Barrel re-export
├── workflows.ts                     # Barrel re-export
├── activities/
│   └── activities.ts                # listDatasets, processDataset
├── workflows/
│   └── discoverProjects.ts          # Orchestration with mode selection
├── schedules/
│   └── scheduleProjectsDiscovery.ts # Temporal cron schedule
└── sources/
    ├── types.ts                     # IDiscoverySource, IDatasetDescriptor
    ├── registry.ts                  # Source factory map
    └── ossf-criticality-score/
        ├── source.ts                # IDiscoverySource implementation
        └── bucketClient.ts          # GCS public bucket HTTP client
```
