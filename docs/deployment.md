# Deployment

The SDK does not connect to Betradar directly. Betradar delivers UOF over AMQP; a RabbitMQ
source connector consumes it and publishes each message to an Apache Pulsar topic, and SDK
instances consume that topic:

```text
Betradar AMQP → RabbitMQ source connector → Pulsar topic → SDK instances
```

This guide covers what to set up and why. The [architecture guide](architecture.md#feed-topology)
explains how the SDK uses the topic, and the [recovery guide](recovery.md) how it keeps
producers synchronized.

- [Checklist](#checklist)
- [Source connector](#source-connector)
- [Topic](#topic)
- [Subscriptions](#subscriptions)
- [SDK configuration](#sdk-configuration)
- [Monitor store](#monitor-store)
- [Scaling and rolling deploys](#scaling-and-rolling-deploys)
- [Verifying a deployment](#verifying-a-deployment)

## Checklist

- [ ] A RabbitMQ source connector with message-key rules, the routing-key property, key-based
      batching, and server-generated consumer tags, pointed at Betradar.
- [ ] A single-partition topic whose backlog is never discarded.
- [ ] The SDK's subscriptions created before the first start, or topic retention.
- [ ] Dead-letter topics that can be created automatically, or created in advance.
- [ ] One `node_id` per base subscription, shared by its instances.
- [ ] A persistent monitor store, with one stable key per instance.
- [ ] `:uof_api` configured for the same Betradar environment as the connector.

## Source connector

### Connect to Betradar

Point the connector at Betradar's AMQP host with your access token as the username, your
assigned virtual host, the `unifiedfeed` exchange, and the `#` binding. Betradar requires
TLS; configure it as the connector's documentation describes.

| Environment | Betradar AMQP host |
| --- | --- |
| Production | `mq.betradar.com` |
| Integration | `stgmq.betradar.com` |
| Replay | `replaymq.betradar.com` |

Do not configure a fixed consumer tag. Each new consume session must get a distinct
server-generated tag (`amq.ctag-…`): the SDK compares tags to detect reconnects, which may have
lost messages. See [consumer tags](recovery.md#consume-sessions).

### Message keys and properties

The SDK needs a connector with `messageKeyRules` and `__rabbitmq_routing_key` support
([apache/pulsar-connectors#140](https://github.com/apache/pulsar-connectors/pull/140)).
The [`Makefile`](../Makefile) pins a connector revision that includes it and builds it from
source. Configure these rules in the source configuration; the first full match wins, and
unmatched keys keep the original routing key:

```json
{
  "messageKeyRules": [
    {
      "regex": "^[^.]*\\.[^.]*\\.[^.]*\\.(?:alive|snapshot_complete)(?:\\..*)?$",
      "replacement": "system"
    },
    {
      "regex": "^(?:[^.:]*\\.)+?([^.:]+:[^.:]+)\\.([^.\\-][^.]*)(?:\\..*)?$",
      "replacement": "$1:$2"
    }
  ]
}
```

These are connector rules, not SDK consumer options. They key event messages by sport-event
URN and system messages as `system`; the [connector contract](architecture.md#connector-contract)
explains why. [`docker-compose.yml`](../docker-compose.yml) shows a complete local source
configuration, with the extra dollar-sign escaping Compose requires.

### Batching

> [!WARNING]
> If batching is enabled for the connector's Pulsar producer, use key-based batching.
> Default batching can combine messages with different keys in one batch, which Pulsar
> routes by the first message's key, breaking the Key-Shared distribution the content
> subscription relies on. SDK consumer settings cannot repair mixed-key batches. See
> [Pulsar's Key-Shared producer requirements](https://pulsar.apache.org/docs/4.2.x/concepts-messaging/).

Select it when creating the source:

```sh
pulsar-admin sources create --batch-builder KEY_BASED ...
```

Alternatively, disable producer batching.

## Topic

Use a single partition: a non-partitioned topic, or a partitioned topic with exactly one
partition. Failover ownership is assigned per partition, so this gives the system
subscription a single active owner.

The backlog must never be discarded. The SDK resumes from it after a restart, and content not
yet processed waits in it:

- Disable message TTL, or set it above the worst-case downtime of all SDK instances.
- Use a `producer_exception` backlog quota policy, so a full backlog stops the connector
  instead of evicting messages.

Retention, which keeps messages after every subscription has acknowledged them, is a separate
setting; see [subscriptions](#subscriptions) for when it is needed.

## Subscriptions

For a base subscription such as `uof-sdk`, the SDK uses two subscriptions on the topic:

| Subscription | Type | Purpose |
| --- | --- | --- |
| `uof-sdk-system` | Failover | Control plane: producer health and recovery, on one active instance |
| `uof-sdk-content` | Key-Shared | Event content, divided across instances by sport event |

The content subscription must exist before the first recovery. Pulsar creates subscriptions
asynchronously, so on a first start, or after the content subscription is deleted, the
system pipeline can request a recovery while the content consumer is still connecting. On a
topic without retention (the broker default), messages acknowledged by every existing
subscription can be deleted once their ledger rolls over, and a content subscription created
afterwards silently misses part of the replay. Use one of these:

- **Create both subscriptions before the SDK first starts** (recommended). The content cursor
  then exists before any recovery, whatever the retention:

  ```sh
  pulsar-admin topics create-subscription persistent://public/default/uof-feed \
    --subscription uof-sdk-content --messageId earliest
  pulsar-admin topics create-subscription persistent://public/default/uof-feed \
    --subscription uof-sdk-system
  ```

- **Or set a retention time on the topic** longer than the content subscription can take to be
  created after the SDK starts, for example one hour. Topic policies must be enabled on the
  broker; `pulsar-admin namespaces set-retention` sets the same policy for a namespace:

  ```sh
  pulsar-admin topicPolicies set-retention persistent://public/default/uof-feed \
    --time 1h --size -1
  ```

  The SDK starts a new content subscription at the earliest retained message
  (`initial_position: :earliest`), so it still reads what the system subscription has
  acknowledged. Retention also sets how much a new content subscription processes before it
  reaches current traffic: a day of retention means a new deployment replays a day of feed.

Once the content subscription exists, do not skip, reset, or expire its cursor: the system
subscription treats everything before its own position as delivered.

A message that fails to decode or process is not retried. About a second later it is diverted
to a dead-letter topic named `<topic>-<subscription>-DLQ`, one for each subscription. The
broker must allow those topics to be created automatically, or you create them in advance.
Acknowledgements are tracked per batch index, so a failure does not redeliver the rest of its
batch; Pulsar 4.2 brokers enable this by default (`acknowledgmentAtBatchIndexLevelEnabled`).

## SDK configuration

Configure the topic and base subscription under `:pulsar`, and the recovery API for the same
Betradar environment as the connector; the [README](../README.md#configuration) lists every
option:

```elixir
config :uof_sdk,
  handler: MyApp.FeedHandler,
  node_id: 1,
  pulsar: [
    host: "pulsar://pulsar.internal:6650",
    topic: "persistent://public/default/uof-feed",
    subscription: "uof-sdk"
  ]

config :uof_api,
  base_url: "https://api.betradar.com/v1",
  auth_token: System.fetch_env!("UOF_ACCESS_TOKEN")
```

Other `:pulsar` options are passed to
[`OffBroadway.Pulsar.Producer`](https://hexdocs.pm/off_broadway_pulsar/OffBroadway.Pulsar.Producer.html),
and `:conn_opts` configures the shared client. The SDK owns the two subscription types and
the system consumer's ownership callback.

`node_id` identifies a subscription, not an SDK instance. Recovery replays are addressed to
the `node_id` that requested them, and both pipelines drop messages addressed to another node:

- All instances sharing a base subscription must use the same positive `node_id`. Otherwise
  Key-Shared can deliver part of a replay to instances that discard it, while the active
  system consumer still accepts its `snapshot_complete`: a silent gap.
- Distinct base subscriptions using the same access token need distinct positive IDs.
- Use `nil` only when a single subscription consumes the access token.

## Monitor store

The monitor persists a small snapshot: the upstream consumer tag, and per producer a recovery
checkpoint and whether it is synchronized. With it, a restarted instance whose upstream
session has not changed resumes from the topic backlog without recovery, and otherwise
recovers incrementally instead of requesting a full snapshot.

The default ETS store does not survive a VM restart. In production, implement a persistent
store; the [architecture guide](architecture.md#implementing-a-store) describes the contract
and includes a PostgreSQL example. Each instance needs its own key, stable across restarts,
such as a StatefulSet pod name:

```elixir
config :uof_sdk, monitor_store: {MyApp.MonitorStore, repo: MyApp.Repo, key: "uof-sdk-0"}
```

## Scaling and rolling deploys

Instances with the same topic, base subscription, and `node_id` share the work: Key-Shared
divides content by sport event, and Failover makes one instance the control plane. Start one
SDK instance per VM.

Each instance keeps its own snapshot. When the active instance stops, the broker promotes
another one, which resumes from its own snapshot if it has one for the current upstream
session, and otherwise recovers; without a checkpoint that is a full recovery. A rolling
deploy that restarts the active instance can therefore trigger recoveries, which count
against Betradar's [rate limits](recovery.md#rate-limits).

## Verifying a deployment

After starting the SDK, every active producer should reach `:up`:

```elixir
UOF.SDK.producers()
#=> [%UOF.SDK.ProducerStatus{id: 1, product: "liveodds", status: :up, ...}, ...]
```

A producer that stays `:recovering` or `:down` usually points at the connector, the topic, or
the recovery API. The [observability](architecture.md#observability) signals and the
[recovery guide](recovery.md#monitoring-recoveries) describe what to check. The
Docker-backed integration test (`make test-integration`) runs this whole path locally against
RabbitMQ, the connector, and Pulsar.
