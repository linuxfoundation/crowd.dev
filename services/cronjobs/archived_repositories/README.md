# Archived repositories checker

This checks if git repositories are archived.

Currently only GitHub and Gitlab are supported.

It gets all the repositories we have in our database, calls the GitHub and GitLab APIs to check if the
repositories are archived, taking care to not go over the APIs rate limits, and updates each repository in the database
accordingly.

It uses the [BullMQ](https://bullmq.io/) library to manage the job queue and concurrency, including the rate limiting.
BullMQ requires a Redis instance to be running, which it uses as storage.

It is meant to be run as a recurring Job in a Kubernetes cluster.

It has two main processes:

- The main process, which is responsible for fetching the repositories from the database and adding them to the
  BullMQ queue.
- The workers process, which is responsible for processing the jobs in the queue, which, for each queued repository,
  involves calling the GitHub and GitLab APIs to check if the repository is archived, and updating the repositories in
  the database.

## How it works

### Selecting repositories

Each run of the main process reads up to `ARCHIVED_REPOSITORIES_CHECKER_BATCH_SIZE` (default 10,000) URLs from
`public.repositories`. It picks rows that are not soft-deleted, whose URL starts with `https://github.com/` or
`https://gitlab.com/`, and that were never checked or were last checked more than 3 days ago. Rows never checked come
first, then the ones checked longest ago.

It enqueues each URL into one of two BullMQ queues, `github-archived-repo-checker` or `gitlab-archived-repo-checker`.
A failed job is retried up to 4 times with exponential backoff.

### Rate limits

Each queue has its own worker, with a concurrency of 5 and a limiter set below the provider's published limit:

| Queue  | Limiter               | Provider limit                          |
| ------ | --------------------- | --------------------------------------- |
| GitHub | 3,500 jobs per hour   | 5,000 requests per hour (authenticated) |
| GitLab | 1,500 jobs per minute | 2,000 requests per minute               |

The limits are set in `src/workers.ts`, not through environment variables.

### Marking archived and excluded

The worker calls the GitHub or GitLab project API and sets two flags:

- `archived` takes the provider's `archived` value, and `excluded` is `false`.
- A 403 or 404 response means the repository is gone or not accessible, so the worker sets `excluded` to `true` and
  `archived` to `false`.
- Repositories named `.github` on GitHub are always set to `excluded`, whatever their archive status.
- Any other error fails the job, and BullMQ retries it.

Each processed repository gets its `archived`, `excluded`, `lastArchivedCheckAt` and `updatedAt` columns updated in
`public.repositories`. The table is created in
[`V1766661879__createRepositoriesTable.sql`](../../../backend/src/database/migrations/V1766661879__createRepositoriesTable.sql).
Sequin replicates it to the Tinybird `repositories` datasource, where health score and security pipes filter on these
flags. [`docs/design/activity-pipeline.md`](../../../docs/design/activity-pipeline.md) shows how Sequin replication
reaches Tinybird.

## Configuration

| Variable                                                                                                                    | Purpose                                                            |
| --------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| `ARCHIVED_REPOSITORIES_CHECKER_GITHUB_TOKEN`                                                                                | GitHub API token                                                   |
| `ARCHIVED_REPOSITORIES_CHECKER_GITLAB_TOKEN`                                                                                | GitLab API token                                                   |
| `CROWD_DB_WRITE_HOST`, `CROWD_DB_READ_HOST`, `CROWD_DB_PORT`, `CROWD_DB_USERNAME`, `CROWD_DB_PASSWORD`, `CROWD_DB_DATABASE` | PostgreSQL connection. Queries and updates both use the write host |
| `CROWD_REDIS_HOST`, `CROWD_REDIS_PORT`, `CROWD_REDIS_USERNAME`, `CROWD_REDIS_PASSWORD`                                      | Redis connection for BullMQ                                        |
| `ARCHIVED_REPOSITORIES_CHECKER_REDIS_DB`                                                                                    | Redis database index                                               |
| `ARCHIVED_REPOSITORIES_CHECKER_BATCH_SIZE`                                                                                  | Optional. URLs enqueued per run (default 10,000)                   |

All variables except the batch size are required; the process exits at startup if one is missing.

### For production

In production in our Kubernetes cluster, the environment variables can be set in a ConfigMap which is loaded into the
container as environment variables.

### For local development

For easier local development, the dotenv package can load environment variables from a `.env` file.

You just need to copy the file `.env.example` to `.env` and fill in the required environment variables.

You need access to a PostgreSQL database with the necessary repositories table, a Redis instance for the tasks queue,
and the GitHub and GitLab API tokens.

`docker-compose.yaml` starts a local PostgreSQL and Redis along with the main and workers containers, which read
`.env` through a volume. The local PostgreSQL starts empty, so create the `public.repositories` table before running
the main process.

## Building and Running

Node 24 is required to build and run this, as well as pnpm for dependency management.

This cronjob is intentionally not part of the root workspace (`!services/cronjobs`).
It has its own lockfile; install from this directory.

Install the dependencies with:

```bash
pnpm install
```

To run the main process:

```bash
pnpm start-main
```

To run the worker process:

```bash
pnpm start-workers
```

If you want to run a development version with tsx and automatic code reloading, use this for the main process:

```bash
pnpm dev-main
```

And this for the worker process:

```bash
pnpm dev-workers
```

## Deployment

There's a Dockerfile that can be used to build a container image.

The Kubernenetes resource files are in our private repository.

The Dockerfile doesn't contain a `CMD` instruction, so you need to specify the command to run when starting the
container.

To build the Docker image, tagged both with a local name and one for the OCI registry, run:

```bash
export DATE_TAG=$(date +%s).$(git rev-parse --short HEAD) && \
echo $DATE_TAG && \
docker build --platform linux/amd64 -f ./Dockerfile \
  --tag archived-repositories-checker:"${DATE_TAG}" \
  --tag sjc.ocir.io/axbydjxa5zuh/archived-repositories-checker:"${DATE_TAG}" .
```

To push the Docker image to the OCI registry, run:

```bash
docker push sjc.ocir.io/axbydjxa5zuh/archived-repositories-checker:"${DATE_TAG}"
```
