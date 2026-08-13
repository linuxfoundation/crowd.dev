
# Tinybird Documentation

## Table of Contents

- [Introduction](#introduction)
- [Journey of Data](#journey-of-data-from-cdp-to-insights)
- [Making Changes to Resources](#making-changes-to-resources)
- [How to Iterate on Data](#how-to-iterate-on-data)
- [Testing Tinybird Pipes Locally](#testing-tinybird-pipes-locally)
- [Creating Backups](#creating-a-backup-datasource-in-tinybird)
- [Glossary](#glossary)

## Introduction

This directory contains documentation CDP and Tinybird integration. **Tinybird** is a real-time analytics database built on ClickHouse that powers our Insights platform with fast, scalable queries on community activity data.

### System Role

Tinybird sits between our Community Data Platform (CDP) backend and the Insights frontend:

1. **Data Ingestion**: Receives data from Postgres (via Sequin → Kafka → Kafka Connect)
2. **Processing**: Enriches, filters, and aggregates activity data using different architectures (Bucketing & Lambda)
3. **Serving**: Provides fast API endpoints for the Insights dashboard and other consumers

## Journey of Data from CDP to Insights

See [dataflow](./dataflow.md) for a visual diagram showing how data flows from CDP Backend through Tinybird to Insights.

## Architecture Overview

We use **two parallel architectures** to process activityRelations data:

### Lambda Architecture 
1) Deduplicates activityRelations without any filtering. Mainly consumed in CDP, and monitoring pipes. Output: `activityRelations_enriched_deduplicated_bucket_0-2_ds`, queried via `activityRelations_enriched_deduplicated_bucket_union` 

2) Used for ingesting pull request event data and merging with existing events. Output: `pull_requests_analyzed`

For details see [lambda-architecture.md](./lambda-architecture.md)

- Filtering: UNFILTERED (includes bots, all activities)
- Used by: Pull requests, CDP pipes, monitoring
- Details: Lambda architecture pattern for deduplication enrichment and pull request processing


### Bucketing Architecture
Produces filtered data (10 buckets) for Insights API queries. For details see [bucketing-architecture.md](./bucketing-architecture.md)

- Output: `activityRelations_deduplicated_cleaned_bucket_*_ds` (10 buckets)
- Filtering: FILTERED (valid members, enabled repos only)
- Used by: Insights API queries
- Details: Hash-based bucketing architecture for parallel processing


### Comparison

The following table compares the two parallel architectures processing activityRelations data:

| Aspect | Lambda Architecture | Bucketing Architecture |
|--------|---------------------|------------------------|
| **Primary Use Case** | Pull requests, CDP, monitoring, member management | Insights API queries |
| **Output Datasource** | pull_requests_analyzed, activityRelations_enriched_deduplicated_bucket_0-2_ds  | activityRelations_deduplicated_cleaned_bucket_0-9_ds |
| **Data Filtering** | UNFILTERED (includes bots, all repos) | FILTERED (valid members, enabled repos) |
| **Partitioning Strategy** | 3 parallel buckets, hash-based | 10 parallel buckets, hash-based |
| **Copy Mode** | Replace (daily per-bucket incremental merge) | Replace (hourly full refresh) |
| **Query Pattern** | Union all buckets (no snapshot filter) | Union all buckets or route to specific bucket |
| **TTL** | No TTL on buckets (replace mode) | No TTL on buckets (replace mode) |
| **Scalability** | Horizontal (add more buckets) | Horizontal (add more buckets) |
| **Dependencies** | Single-table triggers work well | Multi-table dependencies (members, repos) |

**Which activityRelations output to use:**

- Use Bucketing Architecture output (`activityRelations_deduplicated_cleaned_bucket_*_ds`) for: Insights API, project-specific analytics, filtered queries - since each bucket contains a subset of project data, main use-case is project-specific widgets

- **Use Lambda Architecture output** (`activityRelations_enriched_deduplicated_bucket_union`) for: CDP operations, monitoring, any use case requiring complete unfiltered data, where we can not use the filtered buckets

## Making changes to resources
1. Install the **tb client** for classic tinybird
    - 1.1. Create a new virtual environment for pip `python3 -m venv .venv`
    - 1.2. Source the venv `source .venv/bin/activate`
    - 1.3. In this folder run `pip install -r requirements.txt`
    - 1.4 Check installation `tb --version` The version should be 5.x.x
2. Authenticate with tinybird using `tb auth`. You should use your personal token in tinybird.
3. Make sure you are pointing to the correct workspace by `tb workspace ls` and `tb workspace use`
4. Create new endpoints using the staging instance UI
5. Pull schema changes into code using `tb pull` while you're pointing to staging workspace
   1. You may need to use `tb pull --force` to overwrite the existing files, if your changes are not being fetched.
   2. You can also use `tb pull --match <resource_name>` to pull only specific resources.
   3. And you can use a combination of the two previous "tricks": `tb pull --force --match <resource_name>`.
6. Run `libs/tinybird/scripts/format_all.sh` so that our changes conform to the TinyBird format for our TB version.
7. Create PR for changes, make sure Tinybird-CI passes.
8. Once Changes are merged, now point to production instance and `tb push` the latest stuff.
   1. As before with `tb pull`, you may need to use `tb push /path/to/pipe --force` to overwrite the existing files, if your changes are not being pushed.

---

## How to Iterate on Data

ClickHouse is an **append-only** database, so **updates** aren't possible.  
Instead, we **reingest the data with new fields**, and let **deduplication** handle the rest.

👉  For **deduplication** to work properly, whenever we're updating something (either via ui, sql, or some script) the `updatedAt` fields should also be updated. This is the field that ClickHouse uses to decide which version it's going to use on deduplicating via `ReplacingMergeTree` engine.

### Iterating on Activity Fields

Since `activities` **don’t exist in Postgres**, schema iteration must be done **entirely on Tinybird**.
1. **Stop `data-sink-workers`** to prevent new data from arriving in the `activities` datasource.
2. **Update** `activity.datasource` schema in Tinybird to include the new field.
3. **Create a copy pipe**:
   - Source: existing `activity` datasource  
   - Destination: new datasource (e.g., `activities_new`)  
   - Copy pipe definition must populate the new field
4. Once the copy is **completed**:
   - **Delete** the old `activities` datasource  
   - **Rename** `activities_new` to `activities` (**see end note**)
5. **Restart** `data-sink-workers` with the updated code

---

### Iterating on Datasources Replicated by Sequin

**Steps:**
1. **Pause** the related Sequin sink
2. **Run Postgres migration** to add/update/remove fields
3. **Update Tinybird schema**:
   - Delete and recreate the datasource (e.g., `members.datasource`) via `tb push` (**see end note**)
4. **Backfill** the resource from Sequin
5. **Restart** the paused sink

---

### Add new tables to sequin and tinybird

**Steps:**
1. Create the table in postgres (or skip if it already exists)
2. Add migration for the sequin publication to include tables
```sql   
ALTER PUBLICATION sequin_pub ADD TABLE "tableName";
ALTER TABLE public."tableName" REPLICA IDENTITY FULL;
```
3. (only for PROD) You need to create the topic in oracle kafka, it doesn't get created automaticly
4. Update tinybird kafka connect plugin env ( it's under crowd-kube/lf-prod-oracle(lf-staging-oracle)/kafka-connect/tinybird-sink.properties.enc ), there are list of tracked files in the decrypted file.
5. Restart kafka-connect
6. Create tinybird datasource schema and push it to tinybird
7. Add access to the table to sequin user in postgres
```sql
GRANT SELECT ON "tableName" to sequin;
```
8. Create sequin sinks for new tables
9. Create tinybird datasources
10. Backfill from sequin

---

### Downtime Consideration

Switching between old and new datasources can lead to **temporary downtime**, but only for **endpoint pipes that consume raw datasources directly**. 

**No Downtime** if the endpoint pipe uses a **copy pipe result**:
- You can safely remove the raw datasource after stopping the copy pipe
- The copy pipe result datasource will continue to serve data
- New fields will be included in the **next copy run**

**Only consider the following tips if your pipe is consuming raw datasources directly**:

- **Downtime starts** when the old datasource is deleted
- **Downtime ends** when the new datasource is renamed to the old name

👉 It's important to **keep this switch as short as possible** to minimize disruption.

---

# Testing Tinybird Pipes Locally

This guide explains how to test a Tinybird data pipeline ("pipe") on your local Tinybird environment. We will fetch sample data (fixtures) from a staging Tinybird workspace and use it to run and verify a pipe locally. The steps below are written for a developer who may not be familiar with Tinybird, and they are organized in a clear, numbered format for easy follow-up.

## Prerequisites

- **Tinybird CLI and Local Environment:** Make sure you have the Tinybird CLI (`tb`) installed.  
  To start a local Tinybird instance, run the following Docker command:
  ```bash
  docker run --platform linux/amd64 -p 7181:7181 --name tinybird-classic-local \
    -e COMPATIBILITY_MODE=1 -d tinybirdco/tinybird-local:latest
  ```
  This will start Tinybird Local at `http://localhost:7181`.

- **Staging Workspace Access:** You need access to a Tinybird staging (or production) workspace with an API token that has permission to read data (we'll use it to export data).  
- **Project Files:** Ensure you have your Tinybird project files available locally – this includes the data source definitions and pipe (.pipe) files you plan to test. For example, if you plan to test a pipe that uses a data source named `insightsProjects`, you should have the corresponding `insightsProjects.datasource` file (or have created that data source) in your local workspace. Similarly, have the pipe file (e.g. `activities_filtered.pipe` and any related pipes) ready in your project directory.

## Steps to Test the Tinybird Integration Locally

1. **Set Up Environment Variables and Authenticate**  
   Instead of exporting variables directly in the shell (which can leak tokens into your shell history), we recommend using a `.env` file.

   First, copy the example file and fill in the values:
   ```bash
   cp .env.example .env
   ```

   Edit `.env` and set your staging token. For the local token, you need to fetch it from the local Tinybird API with the following command:
   ```bash
   curl -s "http://localhost:7181/tokens" | jq -r '.workspace_admin_token'
   ```
   Copy the output and paste it into the `TB_TOKEN_LOCAL` entry inside `.env`.

   Example `.env`:
   ```bash
   TB_HOST_LOCAL=http://localhost:7181
   TB_TOKEN_LOCAL=eyJhbGciOiJIUzI1NiIsInR5cCI6...

   TB_HOST_STAGING=https://api.us-west-2.aws.tinybird.co
   TB_TOKEN_STAGING=<YOUR_STAGING_TOKEN>

   TB_HOST_PRODUCTION=https://api.us-west-2.aws.tinybird.co
   TB_TOKEN_PRODUCTION=<YOUR_PRODUCTION_TOKEN>
   ```

   Then load the environment variables into your current shell (Linux/macOS, or Windows with Git Bash/WSL):
   ```bash
   set -a
   source .env
   set +a
   ```

   Now you can authenticate with the Tinybird CLI:
   ```bash
   # Authenticate to local Tinybird workspace
   tb auth --host "$TB_HOST_LOCAL" --token "$TB_TOKEN_LOCAL"

   # Authenticate to staging Tinybird workspace
   tb auth --host "$TB_HOST_STAGING" --token "$TB_TOKEN_STAGING"

   # Authenticate to production Tinybird workspace
   tb auth --host "$TB_HOST_PRODUCTION" --token "$TB_TOKEN_PRODUCTION"
   ```

   ⚠️ **Note for Windows PowerShell users:**  
   `source` and `set -a` are not available in PowerShell. We recommend using [dotenvx](https://dotenvx.sh/) or an equivalent approach:  
   ```powershell
   dotenvx run -f .env -- tb auth --host $env:TB_HOST_LOCAL --token $env:TB_TOKEN_LOCAL
   ```

2. **Export Fixture Data from Staging**  
   Next, fetch some sample data from the staging environment to use in your local test. We will use the Tinybird SQL API via a `curl` command to retrieve data. In this example, we select up to 200 rows from the `insightsProjects` data source in the staging workspace and save it to a local file:
   ```bash
   curl -s -H "Authorization: Bearer $TB_TOKEN_STAGING" \
     --data-urlencode "q=SELECT id, name, slug, description, segmentId, createdAt, updatedAt, deletedAt, logoUrl, organizationId, website, github, linkedin, twitter, widgets, repositories, enabled, isLF, keywords FROM insightsProjects LIMIT 200 FORMAT JSONEachRow" \
     "https://api.us-west-2.aws.tinybird.co/v0/sql" \
     | jq -c '{record: .}' > insightsProjects.ndjson
   ```

   ⚠️ Check if this data is not already present in the `fixtures` folder. If not, place the output in the `fixtures` folder and commit it.
   **for production you must use the ${TB_TOKEN_PRODUCTION}

3. **Switch Back to Local Workspace**  
   Now that we have the fixture data, we need to switch the Tinybird CLI context back to the local environment before importing data and pushing pipes:
   ```bash
   tb auth --host "$TB_HOST_LOCAL" --token "$TB_TOKEN_LOCAL"
   ```

4. **Import the Fixture Data into Local Datasource**  
   Append the data you downloaded to the corresponding data source in your local Tinybird workspace:
   ```bash
   tb datasource append insightsProjects --file insightsProjects.ndjson
   ```

5. **Push the Pipe to the Local Environment**  
   Push the Tinybird pipe you want to test into your local Tinybird workspace:
   ```bash
   tb push pipes/activities_filtered.pipe
   ```

6. **Test the Local API Endpoint**  
   Finally, call the pipe’s API endpoint on your local Tinybird instance to verify it works with the local data:
   ```bash
   curl -s "http://localhost:7181/v0/pipes/activities_count.json" \
     -G \
     --data-urlencode "project=umbertotest5" \
     --data-urlencode "token=$TB_TOKEN_LOCAL"
   ```

## Additional Tips

- If the `curl` request in step 6 returns an authentication error, double-check that you're using the correct token.
- You can iterate on your pipe queries locally and rerun the endpoint call to quickly test changes.
- Remember that any changes made locally (like new data or modified pipes) are not automatically reflected in the cloud workspace.

## 🧩 Creating a Backup Datasource in Tinybird

Sometimes you may want to maintain a **logical backup** of a datasource (e.g., `activities`) to ensure data safety, enable testing, or verify deduplication results.

### Steps to Create the Backup

1. **Create a new datasource** — for example:
   ```
   activities_backup.datasource
   ```
   The schema should match the original `activities` datasource.

2. **Create a Materialized View** that automatically mirrors new inserts:
   ```
   activities_backup_mv.pipe
   ```
   This view should insert all incoming rows from `activities` into `activities_backup`.

3. **Deploy both resources** using the following command:
   ```bash
   tb push <pipe_name> --populate --force
   ```
   - `--populate` ensures the datasource is initially filled with the current data.  
   - `--force` guarantees schema and metadata alignment during creation.

> 💡 Tip: `activities_backup` should ideally use a `ReplacingMergeTree` engine with `id` as the sorting key and `updatedAt` as the version column, to handle deduplication properly.

---

### 🔍 Validating Consistency Between `activities` and `activities_backup`

After setup, you can periodically verify that both datasources are logically synchronized.

```bash
# 1. Distinct IDs in activities
tb sql "SELECT uniqExact(id) FROM activities"

# 2. Distinct IDs in activities_backup
tb sql "SELECT uniqExact(id) FROM activities_backup"

# 3. Deduplicated count in activities
tb sql "SELECT count() FROM activities FINAL"

# 4. Deduplicated count in activities_backup
tb sql "SELECT count() FROM activities_backup FINAL"
```

✅ **Consistency is OK** when:
- (1) = (2) → same set of unique IDs  
- (3) = (4) → same number of logical records after deduplication  

If both pairs match, the backup is **logically consistent** with the source dataset.


---

## Health Score v2 Configuration

### Excluding Repositories from Health Score

Repositories can be marked as excluded from health scoring by setting the `repositories.excluded` flag to `1`. This is the official mechanism for marking experimental, sandbox, meta, or otherwise out-of-scope repositories.

**Use cases for exclusion:**
- `.github` meta repositories (configuration-only, no product code)
- Archived experiments or prototypes
- Forks created for testing purposes
- Any repository where activity metrics are meaningless or undesired

**Behavior when excluded:**
- Health Score v2 categorizes excluded repos as `unavailable` for responsiveness scoring (never scored 0 for missing PR/issue data)
- Other category signals continue to be scored normally
- Project-level health scores (when a project contains multiple repos) exclude the marked repo from aggregation

**Setting the flag:**
```sql
UPDATE repositories SET excluded = TRUE WHERE url = 'https://github.com/org/repo-meta';
```

The flag is read by `project_insights_copy.pipe` and `health_score_v2_raw_inputs_snapshot.pipe` as part of their graceful-degradation and audit logic.

---

## Glossary

- **CDP (Community Data Platform)**: Customer data operations and management pipelines
- **Tinybird**: Real-time analytics database built on ClickHouse, used for fast query processing
- **Datasource**: A Tinybird table where data is stored (analogous to database tables)
- **Pipe**: A Tinybird SQL query that can be scheduled or materialized
- **MV (Materialized View)**: A pipe that triggers automatically on INSERT to a datasource
- **Copy Pipe**: A scheduled pipe that copies/transforms data from one datasource to another
- **Sequin**: Database replication tool that streams Postgres changes to Kafka
- **Insights**: The frontend analytics interface for community data
- **segmentId**: Unique identifier for a project/community segment
- **snapshotId**: Timestamp identifier used for deduplication and versioning in lambda architecture
