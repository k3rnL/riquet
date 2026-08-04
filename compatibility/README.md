# Compatibility reference policy

Riquet's required differential suite is pinned to Confluent Platform Schema
Registry 8.3.0. CI pulls the official multi-platform image by immutable index
digest:

```text
confluentinc/cp-schema-registry:8.3.0@sha256:d9e6f3d1142598906a51dca66b8b1251bb0bc24ba7faae1836d7c642fc4d70ea
```

The tag identifies the human-readable reference release and the digest prevents
the required oracle from changing without review. The required suite never
follows `latest`. A scheduled drift workflow may resolve and test the newest
supported Confluent release, but detected differences are reports until the
manifest and pinned digest are changed in a reviewed commit.

The reference image and Confluent Schema Registry server source are governed by
Confluent's applicable terms, including the Confluent Community License for the
server. They are downloaded only into test environments and are not copied into
or redistributed with Riquet artifacts. Client components can have different
licenses; each client fixture must record and audit its own dependency license.

The reference release and current support statement are documented in
`manifest.json`. Compatibility exceptions must be narrow, version-specific, and
linked to an executable scenario. Updating the oracle requires running the full
differential suite and reviewing all manifest changes.

## Kafka broker certification

The initial Kafka HA certification target is Confluent Platform Kafka 8.3.0,
using the immutable test image
`confluentinc/cp-kafka:8.3.0@sha256:c2cedb691aec9963114fb0b4e45fa49a47bb374a89c241c4ecb68a5fc904e5e3`.
The automated suite runs the common storage contract plus transactional
read-committed replay, compaction rebuild, broker restart, network interruption,
lost-response retry, primary handoff, follower convergence, and obsolete
producer fencing.

This is a certification statement, not a vendor lock: Riquet uses Kafka
protocol APIs and does not call Confluent-specific broker APIs. Other Apache
Kafka and compatible broker versions remain unlisted until the identical HA
suite passes against a pinned image. The protocol floor is Kafka 2.5 because
transactions, read-committed fetches, consumer groups, and safe group/transaction
interaction are mandatory.
