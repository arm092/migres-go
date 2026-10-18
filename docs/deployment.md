# Docker and Kubernetes

[Back to README](../README.md)

## Docker

The [local demo](../examples/README.md) includes both databases. For existing databases, the root Compose file builds only Migres:

```sh
cp config.yml.example config.yml
# Edit database endpoints reachable from the container.
export MYSQL_PASSWORD='your-source-password'
export CLICKHOUSE_PASSWORD='your-target-password'
docker compose up -d --build
docker compose exec migres /usr/local/bin/migres status --config /app/config.yml
curl --fail http://127.0.0.1:8080/readyz
```

Inside a container, `127.0.0.1` refers to that container. Use reachable database DNS names or your Docker network service names. Mount TLS CA files separately when configured.

The image runs as user 65532. Compose mounts a persistent `migres-data` volume, applies `restart: unless-stopped`, and allows 90 seconds for termination. The configured application shutdown default is 60 seconds. Keep the volume across restarts and upgrades; deleting it removes checkpoints and ownership history.

## Kubernetes setup

[statefulset.yaml](../deploy/kubernetes/statefulset.yaml) supplies a Service and a single-replica StatefulSet with a ReadWriteOncePod claim, non-root execution, resource requests/limits, probes, and a 90-second termination grace period.

Build and publish your own image. There is currently no automatic release image publication; the manifest image reference must be replaced with an image you have published, preferably an immutable digest.

```sh
docker build -t registry.example.com/migres-go:your-revision .
docker push registry.example.com/migres-go:your-revision
kubectl create namespace migres
kubectl -n migres create configmap migres --from-file=config.yml
kubectl -n migres create secret generic migres \
  --from-literal=mysql-password="$MYSQL_PASSWORD" \
  --from-literal=clickhouse-password="$CLICKHOUSE_PASSWORD"
```

Copy the manifest to your deployment configuration, set its image, and set `volumeClaimTemplates[].spec.storageClassName` if the intended StorageClass is not the default. Then apply that edited file:

```sh
kubectl -n migres apply -f your-migres-statefulset.yaml
kubectl -n migres rollout status statefulset/migres
kubectl -n migres exec migres-0 -- /usr/local/bin/migres status --config /etc/migres/config.yml
kubectl -n migres port-forward service/migres 8080:8080
```

Mount CA files through an additional volume when using TLS. Configure image pull credentials if your registry requires them.

## Storage and upgrades

- Use a CSI driver that supports ReadWriteOncePod and reliable exclusive file locking.
- Keep one replica per journal. More delivery workers are configured inside the process, not by scaling the StatefulSet.
- Validate fencing against the actual storage and node-failure behavior before production.
- Do not force-delete a Pod and start a replacement owner until the former node is fenced.
- Preserve the PVC and deploy image revisions deliberately. Test restore and rollback procedures against the journal format you use.

The sample resource limits are starting points. Align batch sizes, maximum transaction size, memory limits, and disk capacity with the largest source rows and expected outage backlog.

## Probes and observability

| Endpoint | Meaning |
| --- | --- |
| `/livez` | Process health; external database outages alone do not fail liveness |
| `/readyz` | Initial delivery complete and pipeline available; false during blocking failures/rebuilds |
| `/metrics` | Prometheus text metrics |

Relevant metrics include `migres_ready`, `migres_blocked`, `migres_stage_running`, `migres_raw_events`, `migres_prepared_queries`, `migres_journal_bytes`, `migres_storage_bytes`, `migres_staged_events`, `migres_oldest_pending_age_seconds`, `migres_snapshot_running`, `migres_snapshot_complete`, `migres_ddl_running`, `migres_rebuild_running`, and delivery/retry counters. Replicated mode also reports replication probe/queue/delay gauges.

The oldest pending age is a queue-age indicator, not an authoritative source-to-target replication lag. Monitor source binlog retention and validate end-to-end freshness separately. Keep HTTP endpoints on a private network or behind your existing access controls; the built-in control interface is the local Unix socket.
