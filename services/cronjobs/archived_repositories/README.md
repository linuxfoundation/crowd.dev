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

## Configuration

### For production

In production in our Kubernetes cluster, the environment variables can be set in a ConfigMap which is loaded into the
container as environment variables.

### For local development

For easier local development, the dotenv package can load environment variables from a `.env` file.

You just need to copy the file `.env.example` to `.env` and fill in the required environment variables.

You need access to a PostgreSQL database with the necessary repositories table, a Redis instance for the tasks queue, 
and the GitHub and GitLab API tokens.


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
