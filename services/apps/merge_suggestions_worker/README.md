# merge_suggestions_worker

Generates member and organization merge suggestions and runs the LLM review of them. How this
worker fits into deduplication across CDP is in
[`docs/design/deduplication.md`](../../../docs/design/deduplication.md); the files, tables and
workflows are listed in
[`docs/knowledge/flow-deduplication.md`](../../../docs/knowledge/flow-deduplication.md).

## Scoring a member pair

`MemberSimilarityCalculator.calculateSimilarity` returns a score from 0 to 1. It checks the rules
in this order and returns at the first match. The fuzzy path at the end starts from the edit
distance between the closest pair of verified identities and only stays high if at least one
additional check passes.

```mermaid
flowchart TD
    A[Candidate pair] --> B{"Clashing verified usernames on one platform?"}
    B -->|yes| L1[0.2]
    B -->|no| C{"Email verified on one side, unverified on the other?"}
    C -->|yes| H98[0.98]
    C -->|no| D{"An unverified identity of the primary..."}
    D -->|"equals a verified identity"| H98
    D -->|"matches as email-as-username"| H95[0.95]
    D -->|no| N{"A noreply email resolves to the other's GitHub or GitLab username?"}
    N -->|yes| H95
    N -->|no| V{"A verified identity of the primary..."}
    V -->|"equals an unverified identity, or matches as email-as-username"| H95
    V -->|no| E{"Any verified identities to compare?"}
    E -->|no| S90["Start at 0.9"]
    E -->|yes| F{"Edit distance longer than the identity?"}
    F -->|yes| L2[0.2]
    F -->|no| G["Start at 1 - distance / length"]
    S90 --> X{"Any additional check passes?"}
    G --> X
    X -->|no| L3[0.2]
    X -->|yes| Y["Bump by (1 - start) / 6 per check, max 1"]
```

The additional checks are: same location, roles in the same organization, shared languages,
shared programming languages, same timezone, and the same local part in git usernames on
unconfigured machines (`*.local`, `*.lan`). Git username clashes between two email-shaped values
do not count as a clash unless a display name is generic or a member is a bot. Cross-type
matching covers platforms that store emails as usernames: Git, Jira, Confluence and Gerrit.

## Scoring an organization pair

`OrganizationSimilarityCalculator.calculateSimilarity` checks the rules in this order and returns
at the first match. The identity checks run for each verified identity of the primary
organization in turn.

```mermaid
flowchart TD
    A[Candidate pair] --> B{"Same name and shared verified primary domain?"}
    B -->|yes| S85[0.85]
    B -->|no| C{"Clashing identities?"}
    C -->|yes| L1[0.2]
    C -->|no| D{"Verified identity matches an unverified one?"}
    D -->|yes| S95[0.95]
    D -->|no| E{"Same display name?"}
    E -->|"yes, start 0.9"| X
    E -->|no| F{"Similar domain pattern?"}
    F -->|"yes, start 0.85"| X
    F -->|no| V{"Any verified identities to compare?"}
    V -->|"no, start 0.9"| X
    V -->|yes| H{"Edit distance longer than the identity?"}
    H -->|yes| L2[0.2]
    H -->|no| G["Start at 1 - distance / length"]
    G --> Z{"Start is 0? (distance equals length)"}
    Z -->|"yes, start 0.9"| X
    Z -->|no| X{"Same location, industry or ticker?"}
    X -->|no| M[0.75]
    X -->|yes| K[Keep the starting score]
```

A starting score of 0 is replaced by 0.9 (`startingScore || HIGH_CONFIDENCE_SCORE`), so a pair
whose edit distance equals the identity length starts high, not low. The bump per passing check
is rounded down to 0, so a passing check keeps the starting score instead of raising it. A pair that passes no additional check scores exactly 0.75 whatever its
starting score: it lands in `organizationToMergeRaw` but not in `organizationToMerge`. A pair
with a low edit-distance score and a passing check keeps that low score, so it scores lower than
the same pair with no passing check.
