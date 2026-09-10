FROM node:24-alpine as dependencies

WORKDIR /usr/crowd/frontend

COPY package-lock.json package.json ./
RUN npm ci

COPY .browserslistrc .eslintrc.js babel.config.js postcss.config.js tailwind.config.js vite.config.js tsconfig.json index.html ./
COPY ./public ./public
COPY ./src ./src
COPY ./.tailwind ./.tailwind
COPY ./config ./config

FROM dependencies as builder

WORKDIR /usr/crowd/frontend

RUN NODE_ENV=production npm run build:production

FROM nginx:1.23-alpine as release

RUN apk add --no-cache bash

COPY nginx.kube.conf /etc/nginx/nginx.conf
COPY ./scripts/docker-entrypoint.sh /docker-entrypoint.sh
COPY --from=builder /usr/crowd/frontend/dist /etc/nginx/html

ENTRYPOINT [ "/docker-entrypoint.sh" ]
