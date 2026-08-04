# Third-party notices

Riquet links the Go modules recorded in `go.mod` and `go.sum`. Release
qualification permits Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, MIT, and
MPL-2.0 dependencies. The reviewed, release-binary dependency inventory is
maintained in `test/release/dependency-licenses.tsv`; the automated audit fails
if the linked module set changes or a module's license file is absent.

The Confluent Platform Schema Registry and Kafka images are downloaded only by
the compatibility test environment under Confluent's applicable terms. They
are not linked into or redistributed with Riquet source, binaries, containers,
or Helm packages.
