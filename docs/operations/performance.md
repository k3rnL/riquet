# Performance and tested limits

Riquet's release load test uses 500 distinct Avro schemas by default. On the
release qualification host it measured approximately 2,675 registrations/s,
4,492 latest-version lookups/s, 17,602 transition replays/s, and 18,714 follower
catch-up transitions/s. These figures are regression evidence, not an SLA;
storage latency, schema complexity, CPU, broker settings, and network topology
materially affect production results.

Set `RIQUET_LOAD_SCHEMAS` from 1 through 10,000 to size a local qualification:

```sh
RIQUET_LOAD_SCHEMAS=5000 go test ./test/load -run Load -count=20 -timeout=10m -v
```

The release pipeline repeats the default registration, lookup, replay, and
catch-up workload 20 times. The Kafka fault suite additionally exercises
recovery after compaction and broker restart, interrupted and ambiguous commits,
bounded follower lag/readiness, obsolete-primary fencing, and repeated primary
loss. The runtime HA test verifies forwarded writes, convergence, primary
failure, election, and continued mutation service.

Defaults favor bounded resource use: request bodies are limited to 2 MiB;
server read/write timeouts are 30 seconds; shutdown is 15 seconds; and schema
parsers reject excessive depth, reference count, and compiled size. Tune these
limits only after running representative schemas and monitoring HTTP latency,
errors, replay lag, committed/applied positions, and primary churn.
