---
type: design
slug: design-packages
name: OSS packages data across services
repo: crowd.dev
owner: CDP team
depends_on: [sys-cdp]
used_by: [sys-packages]
sources:
  - services/apps/packages_worker/src/bin/
  - services/apps/packages_worker/src/db.ts
  - backend/src/db/packagesDb.ts
  - backend/src/db/packagesTemporal.ts
  - backend/src/api/public/v1/index.ts
  - backend/src/api/public/v1/packages/submitBlastRadiusJob.ts
  - backend/src/api/public/v1/packages/submitMemberContributorsJob.ts
---

# OSS packages data across services

How the packages workers, `packages-db`, the public API and Tinybird connect. What each part is
and where it lives is in [`docs/knowledge/sys-packages.md`](../knowledge/sys-packages.md); how to
run the workers is in
[`services/apps/packages_worker/README.md`](../../services/apps/packages_worker/README.md).

## Components

Every worker writes to `packages-db`, a Postgres instance separate from the CDP database. Two
entry points also read the CDP database: `bq-dataset-ingest`, whose Scorecard job reads CDP's
GitHub repositories and integrations, and `security-contacts-worker`, which runs the security
contacts jobs and resolves contributors' organizations for member contributors. The public API
reads `packages-db` directly and starts the on-demand jobs in the packages Temporal namespace.
Tinybird gets a replica of the main tables, derives `ossPackages_enriched_ds` from them, and
sends its enrichment fields back to `packages` through Kafka. Tinybird is also an input: the git
activity contributor sync in `security-contacts-worker` reads a commit contributor snapshot from
Tinybird (`CROWD_PACKAGES_TB_TOKEN`) before writing to `packages-db`.

```mermaid
flowchart LR
    subgraph sources["External sources"]
        reg["Registry APIs<br/>npm, PyPI, Maven, Cargo,<br/>Go, NuGet, RubyGems, Packagist"]
        bq["deps.dev on BigQuery"]
        osv["OSV"]
        sc["OpenSSF Scorecard API"]
        gh["GitHub"]
        dh["Docker Hub"]
        llm["Amazon Bedrock"]
    end

    subgraph workers["packages_worker entry points"]
        ingest["bq-dataset-ingest<br/>universe, versions, dependencies,<br/>package-repo sweep, scorecard"]
        regw["Registry workers"]
        osvw["osv-worker"]
        enr["github-repos-enricher<br/>dockerhub-sync"]
        scw["security-contacts-worker<br/>and member contributors"]
        crit["criticality-worker"]
        br["blast-radius-worker"]
    end

    pdb[("packages-db")]
    cdpdb[("CDP Postgres")]
    tmp["Packages Temporal namespace"]
    api["Public API<br/>/akrites, /akrites-external"]
    tb[("Tinybird")]
    ins["Insights pipes<br/>health score v2, projects"]
    ak["Akrites"]

    bq --> ingest
    sc --> ingest
    reg --> regw
    osv --> osvw
    gh --> enr
    dh --> enr
    gh --> scw
    llm --> scw
    llm --> br

    ingest --> pdb
    regw --> pdb
    osvw --> pdb
    enr --> pdb
    scw --> pdb
    crit --> pdb
    br --> pdb
    cdpdb --> scw
    cdpdb --> ingest

    tmp --> ingest
    tmp --> regw
    tmp --> osvw
    tmp --> scw
    tmp --> br

    ak --> api
    api --> pdb
    api -->|"start jobs"| tmp
    pdb -.->|replication| tb
    tb -.->|"enrichment, via Kafka"| pdb
    tb --> ins
    tb -->|"contributor snapshot"| scw
```

`github-repos-enricher` and `dockerhub-sync` are plain long-running loops, not Temporal workers,
and `criticality-worker` keeps a connection open while ranking runs from scripts and SQL
functions. Schedules and task queues per entry point are listed in the worker README.

## On-demand jobs

Blast radius analyses and member contributor reports are too slow for a request. The API records
the job in `packages-db`, starts a workflow in the packages Temporal namespace, and returns a job
ID that the client polls. The API uses a separate Temporal client for this
(`backend/src/db/packagesTemporal.ts`), because the workers do not poll the API's default
namespace.

```mermaid
sequenceDiagram
    participant C as Akrites
    participant A as Public API
    participant P as packages-db
    participant T as Packages Temporal
    participant W as packages_worker
    C->>A: POST /akrites-external/blast-radius/jobs
    A->>P: insert pending job row
    A->>T: start workflow
    A-->>C: job ID
    T->>W: run activities
    W->>P: write results, update job status
    C->>A: GET .../jobs/{id}
    A->>P: read job and results
    A-->>C: status and results
```

Blast radius jobs run `analyzeBlastRadius` on the `blast-radius-worker` task queue. Member
contributor jobs follow the same pattern under
`/akrites-external/packages/member-contributors/jobs` and run on the `security-contacts-worker`
task queue. Unlike blast radius, a member contributor submission does not always
create a job: a package that already has contributors is rejected with 409 unless its last job
failed, a submission while a job is active for the package returns that job, and a workflow that
fails to start marks its job failed (`submitMemberContributorsJob.ts`).

One exception to the read path above: for member contributor jobs that resolve repositories with
an LLM (`collectionPath` `llm_repo_resolution`), polling returns the Temporal workflow result
instead of reading `packages-db`, so a completed job can fail as expired once its workflow history
is gone (`getMemberContributorsJob.ts`).
