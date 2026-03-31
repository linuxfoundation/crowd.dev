
<!-- BODY -->

# LFX Community Data Platform (fka crowd.dev) 
## Background story
This project was launched as part of the startup crowd.dev. crowd.dev was acquired by the Linux Foundation in April 2024. Following the acquisition, crowd.dev was renamed to "Community Data Platform" and is now part of the [LFX platform](https://lfx.linuxfoundation.org/).

## About this project
LFX Community Data Platform collects and stores data from across the communities in a single database for data unification, identity resolution, analysis, and activation. By utilizing this tool, the Linux Foundation can effectively identify key contributors and organizations, facilitating more efficient community support.

Key features:
* It consolidates developers' touchpoints with a company or brand.
* It captures data from community platforms, product channels, and commercial channels.
* The data is cleaned, and profiles are matched across platforms and enriched with third-party data.
* The platform provides a unified 360-degree view of developers' engagement, their companies, and their customer journey.


## Getting started
⚠️ This documentation is outdated and needs to be reviewed.

To get started with self-hosting, take a look at our [self-hosting docs](https://docs.crowd.dev/docs/getting-started-with-self-hosting).

#### Deployment with Kubernetes

Our services can be deployed using Kubernetes, as well as a lightweight development environment using Docker. You can read more about it in our [self-hosting docs](https://docs.crowd.dev/docs/deployment).

#### Integrations

We currently support all our integrations for self-hosting. For each one of them, you will need to create your own application. You can see the steps for each integration in our [self-hosting integrations guide](https://docs.crowd.dev/docs/self-hosting).

### Development environment

#### <a name="requirements">Requirements</a>

- [Node](https://nodejs.org/en) v16.16.0
- [Docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/)

#### <a name="getting_started">Getting started</a>

1. Get the monorepo from GitHub

```shell
git clone git@github.com:CrowdDotDev/crowd.dev.git
```

2. Run the start script

```shell
cd scripts
./cli start
```

For hot reloading, you can run

```shell
cd scripts
./cli clean-start-dev
```

For starting services required for insights infra (Tinybird, Sequin and Kafka Connect sink),
you can use the `WITH_INSIGHTS` env variable while running cli commands
```
WITH_INSIGHTS=1 ./cli scaffold up
```

This app will be available at http://localhost:8081

For more information on development, you can <a href="https://docs.crowd.dev/docs/docker-compose-single-machine-development-with-docker-images">check our docs</a>.


## Contribution

There are many ways you can contribute to crowd.dev! Here are a few options:

- Star this repo
- Create issues every time you feel something is missing or goes wrong
- Upvote issues with 👍 reaction so we know what's the demand for a particular issue to prioritize it within the roadmap

If you would like to contribute to the development of the project, please refer to our [Contributing guide](https://github.com/CrowdDotDev/crowd.dev/blob/main/CONTRIBUTING.md).

All contributions are highly appreciated. 🙏

## License

Distributed under the Apache 2.0 License.
