# Security and listener boundaries

Riquet can bind public API, internal forwarding, health, and Prometheus metrics
listeners independently. Bind health and metrics only to trusted operational
networks. In Kafka HA mode the internal listener and shared internal token are
mandatory; network policy should permit it only between Riquet replicas.

Public TLS can terminate in Riquet by configuring both `tls.certFile` and
`tls.keyFile`. TLS may instead terminate at an ingress or service mesh. Forwarded
client addresses are trusted only when the direct peer belongs to a configured
`tls.trustedProxyCidrs` network; an untrusted `X-Forwarded-For` is ignored.

Public authentication modes are `anonymous`, constant-time HTTP Basic, and a
constant-time bearer token. When an administrative token is configured,
administrative PUTs to compatibility/mode and all DELETE operations additionally
require `X-Riquet-Admin-Token`. With no token configured these calls remain
Confluent-compatible and rely on the public authentication boundary. Confluent
enterprise RBAC, ACL resources, and role bindings are outside the compatibility
contract.

Passwords, bearer tokens, internal tokens, and admin tokens are omitted from
configuration diagnostics. Request logs record method, numeric result, and
duration only. They do not include URL paths, subject names, bodies, schemas,
authorization headers, or backend connection settings.
