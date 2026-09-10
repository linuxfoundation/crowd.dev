ARG NODE_VERSION=24
ARG PNPM_CACHE_DIR=/tmp/archived-repositories-cronjob-pnpm-cache

## Builder stage
FROM node:${NODE_VERSION}-alpine AS builder
ARG PNPM_CACHE_DIR

WORKDIR /build
ENV pnpm_config_store_dir=${PNPM_CACHE_DIR}

COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
RUN corepack enable && corepack prepare --activate

# Pre-fetch dependencies and use a Docker cache mount for pnpm to speed up subsequent builds
RUN --mount=type=cache,id=pnpm,target=${PNPM_CACHE_DIR} \
    pnpm fetch --frozen-lockfile

COPY ./ /build

RUN --mount=type=cache,id=pnpm,target=${PNPM_CACHE_DIR} \
    pnpm install --frozen-lockfile && \
    pnpm build

# Clean install of production dependencies only
RUN --mount=type=cache,id=pnpm,target=${PNPM_CACHE_DIR} \
    rm -rf node_modules && \
    pnpm install --prod --frozen-lockfile


## Runner stage
FROM node:${NODE_VERSION}-alpine AS runner
WORKDIR /app

# Add non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

COPY --from=builder --chown=nodejs:nodejs /build/dist /app
COPY --from=builder --chown=nodejs:nodejs /build/node_modules /app/node_modules

# Switch to non-root user
USER nodejs

# Set NODE_ENV
ENV NODE_ENV=production
