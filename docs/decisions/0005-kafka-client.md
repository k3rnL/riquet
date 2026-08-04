# 0005: Kafka client and minimum protocol contract

Status: accepted for v1

## Decision

Riquet pins `github.com/twmb/franz-go` at `v1.18.1`. It is the newest release in
the 1.x line whose module baseline (Go 1.21) is compatible with Riquet's Go
1.22 build. Later franz-go releases require newer Go toolchains (`v1.19.5`
requires Go 1.23.8, `v1.20.3` Go 1.24, and `v1.21.4` Go 1.25).

The Kafka adapter uses the low-level `kgo` and `kmsg` APIs rather than a second
administration library. The selected version provides all required primitives:

- transactional producers with a stable `TransactionalID`, producer epoch
  fencing, `BeginTransaction`, synchronous production, and `EndTransaction`;
- direct full-partition consumption with `ReadCommitted` isolation;
- Kafka consumer-group membership and assignment/revocation callbacks for the
  single-partition primary election;
- TLS dial configuration and pluggable SASL mechanisms; and
- raw metadata, topic creation, configuration, and offset requests used for
  strict startup validation and lag measurement.

Riquet requires a Kafka-compatible broker that supports idempotent production,
transactions, read-committed fetches, and consumer groups. These features are
available in Apache Kafka 2.5 and newer; 2.5 is the protocol floor because it
also avoids the older transaction/group rebalance limitation documented by
franz-go. Actual supported broker products and versions are narrower and are
published only after the automated HA certification suite passes.

## Fencing model

All primaries for one registry stream use the same transactional producer ID.
When a newly elected member initializes that producer, Kafka advances its
producer epoch and fences an obsolete producer. Public success is returned
only after the transition is visible through a read-committed consumer. The
coordination group decides who may initialize the producer; Kafka's producer
epoch is the final write fence if group membership is temporarily ambiguous.

## Security

The adapter accepts a cloned `tls.Config` and one or more franz-go SASL
mechanisms. Authentication data is passed directly to franz-go and is never
included in health details or serialized configuration diagnostics. TLS and
SASL can be used together.

## Consequences

The client is pure Go and does not add librdkafka/CGO. Riquet owns its state
record format, replay validation, election protocol, and compatibility matrix;
upgrading franz-go cannot silently change those contracts. A franz-go upgrade
requires the Kafka storage, fencing, interruption, TLS, and SASL suites to pass
before the pin changes.
