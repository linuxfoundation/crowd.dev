```mermaid
flowchart TD
    %% CM Backend section
    subgraph CM[CM backend]
        connectors["Connectors<br/>(GitHub, Git, Nango, etc.)"]
        Postgres[(Postgres)]
        sequin[Sequin]

        %% CM internal connections
        connectors -->|People and Org data| Postgres
        connectors -->|Activity relations data| Postgres
        Postgres -->|Real-time| sequin
        Kafka1[Kafka with Schema Registry for data contract]
        sequin -->|"Real-time"| Kafka1
    end

    %% Tinybird section
    subgraph Tinybird[Tinybird]
        DS["Datasource<br/>(ReplacingMergeTree)"]
        Pipe[Pipe]
        API[API<br/>with caching]

        DS --> Pipe --> API
    end

    %% Snowflake section
    subgraph Snowflake[Snowflake]
        Dedup[Deduplication]
        Bronze[Bronze]
        Silver[Silver]
        Gold[Gold / Platinum]

        Dedup --> Bronze --> Silver --> Gold
    end

    %% External connections
    Kafka1 -->|"People/Org data<br/>Activity relations<br/>Real-time"| DS
    Kafka1 -->|"Snowflake Kafka connector"| Dedup
    Pipe -->|"Kafka Sink Pipes" | Dedup
    connectors -->|"Activities immutable data <br/>  via Tinybird Events API"| DS
    Gold --> Other[Other products]

    %% Frontend flow
    API --> SSR[SSR Nuxt App]
    SSR --> CDN["CDN (Cloudflare)"]
    CDN --> Browser
```
