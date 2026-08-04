# Avro parser: hamba/avro

Status: accepted for v1 parsing, with Riquet-owned compatibility semantics

Riquet uses `github.com/hamba/avro/v2` v2.27.0. It is the newest selected
release compatible with the project's Go 1.22 baseline. The library provides a
pure-Go parser, named-schema cache, stable canonical rendering, and explicit
schema types without bringing in a serializer server or JVM.

The library is not the compatibility oracle. Riquet owns normalization,
reference graph limits, identity composition, and evolution checks in an
adapter tested against the pinned Confluent registry. Library upgrades must run
the entire Avro corpus and differential suite because a parser's accepted input
or canonical rendering can change public registry behavior.
