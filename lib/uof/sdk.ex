defmodule UOF.SDK do
  @moduledoc """
  Top-level supervisor for the UOF feed SDK.

  This is a *library* supervisor: add it to your own supervision tree rather
  than relying on auto-start, so the feed only connects when your app decides:

      children = [
        # ...
        UOF.SDK
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  Configuration is read from application env (see `UOF.SDK.Config`); pass a
  keyword list as the child argument (`{UOF.SDK, sessions: [:live]}`) to
  override per start.

  It supervises (in order, with `:rest_for_one`) an optional checkpoint store,
  producer monitor, a system-message pipeline, and a content-message pipeline.
  Producer state is managed entirely within `UOF.SDK.ProducerMonitor`'s
  GenServer state — no separate registry process is needed.
  """

  use Supervisor

  alias UOF.SDK.Config
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.ProducerMonitor
  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.Producers
  alias UOF.SDK.SystemPipeline

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc "Current state of every known producer (the health check)."
  @spec producers() :: [Producer.t()]
  defdelegate producers(), to: ProducerMonitor

  @doc "Current state of a single producer by id."
  @spec producer(integer()) :: {:ok, Producer.t()} | :error
  defdelegate producer(id), to: ProducerMonitor

  @doc """
  Manually trigger an odds recovery for a producer. Unlike calling the recovery
  API directly, this goes through the producer monitor: the producer shows as
  recovering and the request is correlated with its `snapshot_complete` and
  stall-guarded, healing exactly like an automatic recovery. Pass `full: true`
  to ignore the checkpoint and request a full snapshot.
  """
  @spec recover(integer(), keyword()) :: :ok | {:error, :already_recovering | :unknown_producer}
  def recover(producer_id, opts \\ []), do: ProducerMonitor.recover(producer_id, opts)

  @impl true
  def init(opts) do
    config = Config.load(opts)

    lifecycle =
      monitor_store_child_specs(config.monitor_store) ++
        [
          {ProducerMonitor,
           producers: Producers.fetch(),
           handler: config.handler,
           inactivity_ms: config.inactivity_seconds * 1_000,
           max_processing_delay_ms: config.max_processing_delay_seconds * 1_000,
           node_id: config.node_id,
           monitor_store: config.monitor_store,
           min_interval_ms: config.min_interval_between_recoveries * 1_000,
           max_recovery_ms: config.max_recovery_time * 1_000,
           recovery_overlap_ms: config.recovery_overlap_seconds * 1_000}
        ]

    Supervisor.init(lifecycle ++ child_specs(config), strategy: :rest_for_one)
  end

  @doc """
  Build the feed pipeline child specs for a resolved `config`. Exposed so the
  normalized transport wiring can be inspected/tested without connecting.
  """
  @spec child_specs(Config.t()) :: [Supervisor.child_spec()]
  def child_specs(%Config{} = config) do
    [
      {SystemPipeline,
       name: SystemPipeline,
       producer: config.system_producer,
       metadata_adapter: config.metadata_adapter,
       routing_key_metadata_key: config.routing_key_metadata_key,
       connection_token_metadata_key: config.connection_token_metadata_key,
       monitor: ProducerMonitor},
      {ContentPipeline,
       name: ContentPipeline,
       handler: config.handler,
       concurrency: config.concurrency,
       producer: config.content_producer,
       metadata_adapter: config.metadata_adapter,
       routing_key_metadata_key: config.routing_key_metadata_key,
       connection_token_metadata_key: config.connection_token_metadata_key,
       monitor: ProducerMonitor}
    ]
  end

  @doc false
  @spec monitor_store_child_specs(module()) :: [module()]
  def monitor_store_child_specs(store) do
    if Code.ensure_loaded?(store) and function_exported?(store, :child_spec, 1), do: [store], else: []
  end
end
