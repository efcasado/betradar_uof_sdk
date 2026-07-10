defmodule UOF.SDK.CheckpointStore do
  @moduledoc """
  Behaviour for persisting the last-seen feed timestamp per producer.

  On recovery the SDK uses the stored timestamp as `after:` for an *incremental*
  recovery (`UOF.API.Recovery.recover/2`) instead of a full snapshot. The
  timestamp is **milliseconds since the Unix epoch** — the same unit feed
  messages carry and `recover/2` expects.

  The bundled `UOF.SDK.CheckpointStore.ETS` is a `GenServer` that owns a public
  ETS table (fast, zero-dependency, lost on VM restart), so the SDK starts it
  automatically. A custom adapter that needs no process of its own (for example
  one backed by an existing Ecto repo) only needs to implement this behaviour;
  its external dependencies should be supervised by the host application.

  If a custom adapter does need its own process, expose `child_spec/1` and the
  SDK will add it to its supervision tree before the producer monitor.

      config :uof_sdk, checkpoint_store: MyApp.PostgresCheckpointStore
  """

  @type producer_id :: integer()

  @typedoc "Milliseconds since the Unix epoch (UTC)."
  @type timestamp :: integer()

  @doc "Fetch the stored timestamp for `producer_id`, or `:none`."
  @callback get(producer_id) :: {:ok, timestamp} | :none

  @doc "Store `timestamp` as the latest checkpoint for `producer_id`."
  @callback put(producer_id, timestamp) :: :ok

  @doc "Drop the checkpoint for `producer_id` (forcing a full recovery next time)."
  @callback delete(producer_id) :: :ok

  @doc "Optional child spec for stores that own a process."
  @callback child_spec(term()) :: Supervisor.child_spec()

  @optional_callbacks child_spec: 1
end
