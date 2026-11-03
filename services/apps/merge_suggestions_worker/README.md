# merge_suggestions_worker

Generates member and organization merge suggestions and runs the LLM review of them. How this
worker fits into deduplication across CDP is in
[`docs/design/deduplication.md`](../../../docs/design/deduplication.md); the files, tables and
workflows are listed in
[`docs/knowledge/flow-deduplication.md`](../../../docs/knowledge/flow-deduplication.md).

## Scoring a member pair

`MemberSimilarityCalculator` returns a score from 0 to 1. The first rule that matches decides.
The fuzzy path starts from the edit distance between the closest pair of verified identities and
only stays high if at least one additional check passes.

```mermaid
flowchart TD
    A[Candidate pair] --> B{"Clashing verified usernames on one platform?"}
    B -->|yes| L1[0.2]
    B -->|no| C{"Email matches across verified and unverified?"}
    C -->|yes| H98[0.98]
    C -->|no| D{"Unverified identity matches a verified one?"}
    D -->|"primary unverified = similar verified"| H98
    D -->|"email-as-username or noreply match"| H95[0.95]
    D -->|"primary verified = similar unverified"| H95
    D -->|no| E["Closest verified identities by edit distance"]
    E --> F{"Distance longer than the identity?"}
    F -->|yes| L2[0.2]
    F -->|no| G["Start at 1 - distance / length"]
    G --> X{"Any additional check passes?"}
    X -->|no| L3[0.2]
    X -->|yes| Y["Bump by (1 - score) / 6 per check, max 1"]
```

The additional checks are: same location, roles in the same organization, shared languages,
shared programming languages, same timezone, and the same local part in git usernames on
unconfigured machines (`*.local`, `*.lan`). Git username clashes between two email-shaped values
do not count as a clash unless a display name is generic or a member is a bot. Cross-type
matching covers platforms that store emails as usernames: Git, Jira, Confluence and Gerrit.

## Scoring an organization pair

`OrganizationSimilarityCalculator` follows the same pattern with organization signals.

```mermaid
flowchart TD
    A[Candidate pair] --> B{"Same name and shared verified primary domain?"}
    B -->|yes| S85[0.85]
    B -->|no| C{"Clashing identities?"}
    C -->|yes| L[0.2]
    C -->|no| D{"Verified identity matches an unverified one?"}
    D -->|yes| S95[0.95]
    D -->|no| E{"Same display name?"}
    E -->|yes| X
    E -->|no| F{"Similar domain pattern?"}
    F -->|"yes, start 0.85"| X
    F -->|no| G["Start at edit-distance score"]
    G --> X{"Same location, industry or ticker?"}
    X -->|no| M[0.75]
    X -->|yes| K[Keep the starting score]
```

An organization pair that passes no additional check scores exactly 0.75, so it lands in
`organizationToMergeRaw` but not in `organizationToMerge`.
