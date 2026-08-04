# JSON Schema validator and draft behavior

Riquet v1 pins `github.com/santhosh-tekuri/jsonschema/v6` v6.0.2. The module is
Apache-2.0 licensed, declares Go 1.21, supports drafts 4, 6, 7, 2019-09, and
2020-12, and passes the upstream JSON Schema test suite for those drafts.

The engine follows Confluent's observed behavior by using draft 7 when `$schema`
is absent while honoring an explicit supported dialect. Registry references are
installed as compiler resources and graph traversal is bounded. Default identity
preserves compact JSON property order; `normalize=true` uses recursively sorted
object keys. The `json-api` and JSON evolution corpus compare canonicalization,
references, content-model behavior, and all policies with Confluent 8.3.0.
