# Protobuf parser and descriptor engine

Riquet v1 pins `github.com/bufbuild/protocompile` v0.14.1 for source parsing,
import resolution, linking, and descriptors, with
`github.com/jhump/protoreflect/v2` v2.0.0-beta.2 for stable source rendering.
Both modules declare Go 1.21 and are compatible with Riquet's Go 1.22 baseline.

The engine compiles references from the registry resolver, includes protoc's
standard imports, bounds source size and graph traversal, and retains linked
descriptors for evolution checks. Canonical rendering and GUID inputs are
verified through `protobuf-api`; the mutation corpus verifies all compatibility
policies against Confluent Platform 8.3.0.
