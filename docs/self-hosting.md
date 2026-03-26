# Self-Hosting LFX Community Data Platform (CDP)

This guide provides technical instructions for self-hosting the Community edition of the LFX Community Data Platform (formerly crowd.dev) using Docker Compose.

## Prerequisites

Before you begin, ensure your system meets the following requirements:

### Hardware Requirements
- **CPU**: 4+ cores recommended.
- **RAM**: 16GB+ recommended (the platform runs several services including OpenSearch, Kafka, and Postgres).
- **Storage**: 50GB+ of free space (depends on the volume of data you plan to ingest).

### Software Requirements
- **Docker**: Engine 20.10+ and Docker Compose v2.0+.
- **Node.js**: v16.16.0 (required for some local CLI utilities).
- **Python**: 3.x (required for Tinybird CLI setup).

## Getting Started

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/linuxfoundation/crowd.dev.git
   cd crowd.dev
   ```

2. **Install Dependencies**:
   ```bash
   pnpm install
   ```

3. **Configure Environment Variables**:
   Copy the distribution template to create your local override file:
   ```bash
   cp backend/.env.dist.local backend/.env.override.local
   ```
   *Note: For a "composed" environment where services run in Docker, you may also want to reference `backend/.env.dist.composed` for the correct hostnames (e.g., `db` instead of `localhost`).*

## Deployment Steps

The platform uses a custom CLI tool located in `scripts/cli` to simplify management.

### 1. Start Infrastructure (Scaffold)
The "scaffold" includes essential services: PostgreSQL, Redis, OpenSearch, Kafka, and Tinybird.

```bash
cd scripts
./cli scaffold up
```

This command will:
- Set up the `crowd-bridge` Docker network.
- Start all infrastructure containers defined in `scripts/scaffold.yaml`.
- Set up the Tinybird CLI and virtual environment.

### 2. Run Database Migrations
Once the infrastructure is up, apply the necessary schema migrations for Postgres and Tinybird:

```bash
./cli migrate-up
```

### 3. Start Application Services
You can start all application services (API, Frontend, Workers) at once:

```bash
./cli service up-all
```

The application will be available at:
- **Frontend**: http://localhost:8081
- **API**: http://localhost:4000/api (default)

## Configuration Details

Key environment variables to review in `backend/.env.override.local`:

- `CROWD_API_URL`: The public-facing URL of your API.
- `CROWD_API_JWT_SECRET`: A long, random string for securing sessions.
- `CROWD_DB_*`: Database connection details (defaults work with `scaffold`).
- `CROWD_REDIS_*`: Redis connection details.
- `CROWD_TINYBIRD_BASE_URL`: URL for the local Tinybird instance (default: `http://localhost:7181/`).

## Operations & Maintenance

### Checking Logs
To view logs for a specific service:
```bash
./cli service <service-name> logs
```
*(Available service names can be found in `scripts/services/`)*

### Backing up the Database
```bash
./cli db-backup <backup-name>
```

### Restoring a Backup
```bash
./cli db-restore <backup-name>
```

## Security & Production Notes

- **Reverse Proxy**: It is highly recommended to use a reverse proxy (like Nginx) to handle SSL/TLS termination. A template is provided in `scripts/scaffold/nginx/`.
- **Secrets**: Ensure `CROWD_API_JWT_SECRET` and all database passwords are changed from their default values in a production environment.
- **Persistence**: All data is stored in Docker volumes (e.g., `pgdata-dev`, `opensearch-dev`). Ensure these volumes are backed up regularly.

## Troubleshooting

- **Memory Issues**: If services fail to start, check if Docker has enough memory allocated (especially on macOS/Windows).
- **Network Conflicts**: Ensure the subnet `10.90.0.0/24` (default) does not conflict with your existing network. You can override this in `scripts/cli` or via environment variables.
