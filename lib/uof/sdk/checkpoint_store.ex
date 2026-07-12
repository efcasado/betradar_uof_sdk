defmodule UOF.SDK.CheckpointStore do
  @moduledoc """
  Behaviour for persisting recovery checkpoints, producer state and connection
  tokens across restarts.

  ## Checkpoints

  On recovery the SDK uses the stored timestamp as `after:` for an *incremental*
  recovery (`UOF.API.Recovery.recover/2`) instead of a full snapshot. The
  timestamp is **milliseconds since the Unix epoch** — the same unit feed
  messages carry and `recover/2` expects. Checkpoints only advance from
  subscribed `alive` messages while the producer is up, so a stored checkpoint
  also proves the producer was in sync at that feed time.

  ## Producer state and connection tokens

  `UOF.SDK.ProducerMonitor` persists each producer's `down?`/`delayed?` flags on
  every status transition and the per-pipeline connection token whenever it
  changes, then restores both at startup:

    * A producer whose persisted state shows the remote feed was healthy at
      shutdown (up, or down only because local processing lagged) — and that has
      a checkpoint — resumes as *delayed* instead of triggering the usual
      startup recovery. It returns up once processing catches back up.
    * Restored connection tokens are compared against the tokens observed on
      incoming messages. A matching token means the same upstream consume
      session — no delivery gap, no recovery. A changed token (AMQP transports
      always mint a new consumer tag on restart) recovers every producer.

  Together with a transport that retains messages while the app is down (e.g.
  Pulsar durable subscriptions), this lets deployments restart without
  triggering unnecessary odds recoveries.

  The bundled `UOF.SDK.CheckpointStore.ETS` is a `GenServer` that owns a public
  ETS table (fast, zero-dependency, lost on VM restart), so the SDK starts it
  automatically. Because it outlives monitor/pipeline crashes within the same
  VM, it still provides crash-restart resume; only a VM restart falls back to
  full recovery. A custom adapter that needs no process of its own (for example
  one backed by an existing Ecto repo) only needs to implement this behaviour;
  its external dependencies should be supervised by the host application.

  If a custom adapter does need its own process, expose `child_spec/1` and the
  SDK will add it to its supervision tree before the producer monitor.

      config :uof_sdk, checkpoint_store: MyApp.PostgresCheckpointStore
  """

  @type producer_id :: integer()

  @typedoc "Milliseconds since the Unix epoch (UTC)."
  @type timestamp :: integer()

  @typedoc "Durable subset of `UOF.SDK.Producer` health flags."
  @type producer_state :: %{down?: boolean(), delayed?: boolean()}

  @typedoc "Pipeline namespace a connection token belongs to (`:system` | `:content`)."
  @type namespace :: atom()

  @doc "Fetch the stored timestamp for `producer_id`, or `:none`."
  @callback get(producer_id) :: {:ok, timestamp} | :none

  @doc "Store `timestamp` as the latest checkpoint for `producer_id`."
  @callback put(producer_id, timestamp) :: :ok

  @doc "Drop the checkpoint for `producer_id` (forcing a full recovery next time)."
  @callback delete(producer_id) :: :ok

  @doc "All persisted producer states, keyed by producer id. Empty map when none."
  @callback get_state() :: %{producer_id => producer_state}

  @doc "Store the durable health flags for `producer_id`."
  @callback put_state(producer_id, producer_state) :: :ok

  @doc "All persisted connection tokens, keyed by namespace. Empty map when none."
  @callback get_connection_tokens() :: %{namespace => term()}

  @doc "Store the last-seen connection token for `namespace`."
  @callback put_connection_token(namespace, term()) :: :ok

  @doc "Optional child spec for stores that own a process."
  @callback child_spec(term()) :: Supervisor.child_spec()

  @optional_callbacks child_spec: 1
end
