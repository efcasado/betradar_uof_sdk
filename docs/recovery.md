# Recovery

Betradar's producers can only be followed reliably if the SDK notices when it may have missed
messages and asks Betradar to resend them. This guide explains how the SDK does that on its own,
how to request a recovery yourself, and what to watch in production.

- [Producer status](#producer-status)
- [When the SDK recovers](#when-the-sdk-recovers)
- [How a recovery runs](#how-a-recovery-runs)
- [Interrupted and stalled recoveries](#interrupted-and-stalled-recoveries)
- [Consume sessions](#consume-sessions)
- [Restarts](#restarts)
- [Control-plane ownership](#control-plane-ownership)
- [Time controls](#time-controls)
- [Manual recovery](#manual-recovery)
- [Rate limits](#rate-limits)
- [Monitoring recoveries](#monitoring-recoveries)

## Producer status

Each Betradar producer (live odds, prematch, virtual sports, and so on) has a lifecycle status,
derived from two facts: whether the producer is heard (an `alive` within `inactivity_seconds`),
and whether it owes a recovery.

| Status | Meaning |
| --- | --- |
| `:down` | Not heard: no `alive` read yet since startup or promotion, or the producer went silent |
| `:recovering` | Heard, and owes a recovery: pending, being requested, or waiting for completion |
| `:up` | Heard, and the topic carries a continuous feed for the producer |

Query it at any time, or react to changes in `handle_producer_status/1`:

```elixir
UOF.SDK.producers()
#=> [%UOF.SDK.ProducerStatus{id: 1, product: "liveodds", status: :up, ...}, ...]

UOF.SDK.producer(1)
#=> {:ok, %UOF.SDK.ProducerStatus{...}}
```

`:up` describes [feed continuity](architecture.md#feed-continuity) on the topic, not
application progress. Replayed or live content can still be waiting in the content
subscription, and content reaches your handlers whatever the status. If your business logic
must not act until a replay has been processed, measure that from the feed timestamps your
callbacks receive.

## When the SDK recovers

Automatic recovery normally needs no application involvement. A producer owes a recovery when:

| Trigger | Why |
| --- | --- |
| It has never been synchronized | Initial synchronization, at startup or for a producer added later |
| An `alive` with `subscribed=0` | Betradar reports the producer was down; it sends unsubscribed alives until a recovery is requested |
| No `alive` within `inactivity_seconds`, or two consecutive alives generated more than `inactivity_seconds` apart | The producer or the feed went silent. Betradar generates alives every 10 seconds; alives read from a backlog arrive close together however far apart they were generated, so their timestamps are compared too |
| A new upstream consumer tag | The connector's session to Betradar was re-established and messages may have been lost; every producer owes a recovery |
| A restart that cannot resume | See [restarts](#restarts) |

The SDK records the recovery at once, but requests it only while the producer is heard: an
`alive` within `inactivity_seconds`. Betradar rejects recovery for a silent producer, so until
then the producer is `:down`; its next `alive` requests the recovery.

## How a recovery runs

1. A trigger creates recovery intent for the producer, which is reported as `:recovering`, or
   `:down` while the producer is not heard.
2. The monitor persists that the producer requires recovery before any request is made, so a
   crash at any point cannot later resume from stale state.
3. Once this instance owns the control plane, the producer is heard, and the per-producer
   cooldown (`min_interval_between_recoveries`) has passed, the SDK requests the recovery from
   Betradar's API with a fresh request ID and this subscription's `node_id`.
4. Betradar replays messages over the feed. They reach your handlers like live messages.
5. A `snapshot_complete` carrying the request ID ends the recovery, and the producer is `:up`.

**Incremental or full.** While a producer is `:up`, each subscribed system `alive` advances
its stored checkpoint, and a completed recovery advances it to the last subscribed `alive`
seen while the request ran. Each incremental request starts 60 seconds before that timestamp to
allow for short publication reorderings in key-based Pulsar batches. The checkpoint itself
does not move back. The overlap can replay duplicates; it cannot guarantee recovery of every
message delayed longer than 60 seconds. Without a checkpoint, for example on a first start
with an empty store, the SDK requests a full odds recovery: the current odds, rather than a
message history. [Betradar's recovery API](https://docs.sportradar.com/uof/error-handling/recovery-using-api)
may send only the latest odds change for an event.

**Recovery window.** Each producer limits how far back a recovery may reach (its stateful
recovery window, from the producer descriptions). At each request attempt, including retries,
the SDK moves an `after` timestamp near the window edge to 10 minutes inside the limit to
allow for clock skew and request latency. Older messages then fall outside the replay. Use
[per-event recovery](#recovering-a-single-event) for events you know were affected.

**Request IDs** come from a per-producer sequence starting at a random offset, so a completion
for an older request, issued before a restart or by another instance, does not complete a
newer one.

**Failures.** A rejected or failed request (an API error, an HTTP transport error, or a
response other than `ACCEPTED`) is retried after `min_interval_between_recoveries`. An
exception while issuing it crashes the monitor; the persisted intent makes the restarted
monitor recover again.

## Interrupted and stalled recoveries

**Interrupted.** An unsubscribed `alive`, or a heartbeat gap, while a request is in flight
means the producer may have lost messages the running replay does not cover. The SDK emits
`[:uof_sdk, :recovery, :interrupted]`, lets the recovery finish, and then repeats it from the
last subscribed `alive` seen before the interruption: the completed replay and those alives
show the feed was continuous up to there, so the checkpoint advances to it. Without such an
alive, the recovery repeats from the unchanged checkpoint. The producer stays `:recovering`
until a recovery completes without interruption. For a pending full recovery, the repeat
remains full. The incremental behavior follows the official SDKs.

**Stalled.** A request with no `snapshot_complete` within `max_recovery_time` is abandoned,
with a warning log and `[:uof_sdk, :recovery, :stalled]`, and reissued from the checkpoint
like any owed recovery: once the cooldown has passed and the producer is heard. Reissuing also
covers an earlier interruption. An interrupted recovery that never completes is reissued this
way.

## Consume sessions

RabbitMQ gives every consume session a consumer tag (`amq.ctag-…`). The source connector
copies its session's tag into every message, and the system pipeline reports it with each
`alive`. The monitor checks the tag before it applies the `alive`, so a producer is never
heard, and no recovery is requested for it, in a session that has not been checked. A tag the SDK has not seen before means the connector reconnected to Betradar and may
have missed messages, so every producer owes a recovery. Tags are compared by novelty, not arrival
order, so a late message from an old session is not a change.

## Restarts

The monitor persists the last consumer tag and, per producer, its checkpoint, whether it was
synchronized, and whether a full recovery is pending. After a restart:

- A synchronized producer with a checkpoint resumes without recovery. It is `:down` until its
  first `alive` with the same consumer tag, then `:up`: the upstream session never broke, so
  the backlog on the topic is complete. Status describes the topic as far as the system
  pipeline has read it, so an `alive` drained from the backlog counts; a session change or
  unsubscribed `alive` still queued behind it triggers a recovery when it is read, as during
  normal operation.
- That first `alive` is compared with the checkpoint. One generated more than
  `inactivity_seconds` after it means the producer went silent while the SDK was down, and
  the producer recovers from the checkpoint instead.
- A different consumer tag means the session broke while the SDK was down; each producer
  recovers from its checkpoint unless a full recovery is pending.
- A producer without a synchronized checkpoint starts `:down` and recovers on its first
  `alive`.
- A pending full recovery remains full, even if the producer has an older checkpoint. It is
  cleared only after a matching `snapshot_complete` completes the recovery.
- A producer not heard within `inactivity_seconds` of the restart owes a recovery, as after
  any heartbeat gap.

With the default ETS store this only survives monitor and pipeline restarts, not VM restarts;
see [monitor store](deployment.md#monitor-store).

## Control-plane ownership

Only the instance that owns the Failover system subscription issues recovery requests and
writes the snapshot. The others process their share of content and ignore system
observations. When ownership moves, the promoted instance restores producers from the
snapshot exactly as on a [restart](#restarts): in an unchanged upstream session it resumes
without recovery, and a recovery the snapshot owes, including a full one, is requested once
the producer is heard. Recoveries the demoted instance had in flight are abandoned; the
snapshot already records that they are owed. This needs a store shared by the instances; see
[monitor store](deployment.md#monitor-store).
If the active system consumer exits without reporting demotion, the monitor detects its exit
and demotes. A replacement consumer's ownership report determines whether the instance
resumes as active or stays passive.

## Time controls

| Setting | Default | Effect |
| --- | --- | --- |
| `inactivity_seconds` | `20` | Heartbeat timeout: the longest wait for an `alive`, also measured from a restart or promotion, and the longest gap between consecutive alives' generation timestamps |
| `min_interval_between_recoveries` | `30` | Per-producer cooldown between recovery requests, and the retry delay after a failed one |
| `max_recovery_time` | `600` | Wait for `snapshot_complete` before reissuing a request |

The official SDKs accept 600–3600 seconds for `max_recovery_time`. A lower value can abort a
large recovery that is still replaying, and each reissue starts it again and counts against
the [rate limits](#rate-limits).

Heartbeat inactivity uses local observation time, and the recovery window is applied against
the local wall clock; the cooldown uses monotonic time. Durations are not hard real-time
deadlines: checks run on the monitor's tick. A heartbeat gap is also judged when the `alive`
or `snapshot_complete` that ends it arrives, so a gap that ends between two ticks still counts.

## Manual recovery

### Recovering a producer

`UOF.SDK.recover/2` asks the monitor to recover a producer. It follows exactly the same path
as an automatic recovery: persisted intent, the cooldown, the `:recovering` status, completion
by `snapshot_complete`, interruption handling, and the stall check.

```elixir
UOF.SDK.recover(1)
#=> :ok

UOF.SDK.recover(1, full: true)
#=> :ok
```

| Result | Meaning |
| --- | --- |
| `:ok` | The recovery is requested, or recorded to be requested once the producer is heard and the cooldown allows |
| `{:error, :already_recovering}` | The producer already owes a recovery that covers this one: any recovery for a plain request, a full one for `full: true` |
| `{:error, :passive}` | This instance does not own the control plane; call it on the active instance |
| `{:error, :unknown_producer}` | No such producer in the loaded descriptions |

By default, a manual recovery uses the same 60-second overlap and recovery-window bound as an
automatic one. Use it when you suspect the SDK missed messages that it did not detect on its
own.

### Forcing a full recovery

`full: true` ignores the checkpoint and requests a full odds recovery: the current odds for the
producer, not the messages since a point in time. Use it when your application's own state no
longer matches the feed and you need to rebuild it from current odds, for example after
restoring your database from an older backup or after fixing a handler bug that corrupted
stored odds. If the producer already owes an incremental recovery, `full: true` upgrades it,
abandoning any incremental request in flight. The full request is repeated as full if it is
interrupted by a consume-session change or monitor restart; it is not converted to an
incremental request from an older checkpoint.

Betradar describes this as a recovery of odds; for the settlements, cancellations, and
rollbacks of specific events, use [stateful message recovery](#recovering-a-single-event).
A full recovery is also the most expensive kind and counts against Betradar's
[rate limits](#rate-limits), so do not use it routinely. Betradar's SDK documentation does not
offer full recovery on demand; the SDK exposes it for these rebuild cases.

### Recovering a single event

For gaps limited to known sport events, use Betradar's per-event endpoints through `uof_api`:
`UOF.API.Recovery.recover_event/4` resends the odds for one event, and
`UOF.API.Recovery.recover_stateful_messages/4` its settlements, cancellations, and rollbacks.
Pass the SDK's `node_id` so the replay is addressed to this subscription, and leave out
`request_id`:

```elixir
UOF.API.Recovery.recover_stateful_messages("liveodds", "sr:match:12345", node_id: 1)
```

The replayed messages reach your handlers through the feed. The SDK does not track these
requests and the producer's status does not change. A completion without a request ID never
matches a producer recovery; one with a request ID could, by chance, match the monitor's
in-flight request for that producer and end it early.

## Rate limits

Betradar limits recovery requests by how far back they reach
([restrictions for odds recovery](https://docs.sportradar.com/uof/api-and-structure/api/odds-recovery/restrictions-for-odds-recovery)):

| Recovery length | Limit |
| --- | --- |
| Under 30 minutes | 20 per 10 minutes, 60 per hour |
| 30 minutes to a day | 4 per 10 minutes, 10 per hour |
| A day or more | 2 per 30 minutes, 4 per 2 hours |
| Single events | 100 per 10 minutes, 300 per hour |

A rate-limited client is blocked from new recoveries for up to the period of the limit it hit;
messages and accepted recoveries keep flowing. A rejected request counts as a failure and is
retried after the cooldown.

## Monitoring recoveries

Every recovery event carries the producer ID, product, and request ID, so a recovery's
initiation, failures, interruptions, stalls, and completion can be correlated:

| Event | Emitted when |
| --- | --- |
| `[:uof_sdk, :recovery, :initiated]` | Betradar accepted a request; includes the replay start |
| `[:uof_sdk, :recovery, :completed]` | A matching `snapshot_complete` arrived; includes `duration_ms` |
| `[:uof_sdk, :recovery, :failed]` | A request was rejected or failed; includes the reason and `retry_in_ms` |
| `[:uof_sdk, :recovery, :interrupted]` | An unsubscribed `alive` or heartbeat gap while a request was in flight; includes `reason` |
| `[:uof_sdk, :recovery, :stalled]` | No `snapshot_complete` within `max_recovery_time`; emitted before the reissue |
| `[:uof_sdk, :producer, :status]` | A producer's status changed |
| `[:uof_sdk, :producer, :alive]` | A system `alive` was read; `delay_ms` is how late, which shows how far behind the system pipeline reads the topic |

Alert on a producer that stays `:recovering` or `:down`, on recoveries without `:completed`,
and on repeated `:failed`, `:interrupted`, or `:stalled` events. A growing alive `delay_ms`
means the control plane is falling behind the topic, which delays heartbeat and recovery
detection. The
[architecture guide](architecture.md#observability) lists every signal the SDK emits.
