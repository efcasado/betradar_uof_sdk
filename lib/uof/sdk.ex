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

  It supervises (in order) the checkpoint store, producer registry, recovery
  orchestrator, producer monitor, and one `UOF.SDK.Pipeline` per configured
  session — so the lifecycle components are ready before messages flow.
  """

  use Supervisor

  alias UOF.SDK.{
    Config,
    Pipeline,
    Producer,
    ProducerMonitor,
    ProducerRegistry,
    Producers,
    Recovery,
    Session
  }

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @doc "Current state of every known producer (the health check)."
  @spec producers() :: [Producer.t()]
  defdelegate producers(), to: ProducerRegistry, as: :all

  @doc "Current state of a single producer by id."
  @spec producer(integer()) :: {:ok, Producer.t()} | :error
  defdelegate producer(id), to: ProducerRegistry, as: :get

  @impl true
  def init(opts) do
    config = Config.load(opts)

    lifecycle = [
      config.checkpoint_store,
      ProducerRegistry,
      {Recovery,
       node_id: config.node_id,
       checkpoint_store: config.checkpoint_store,
       min_interval_ms: config.min_interval_between_recoveries * 1_000,
       max_recovery_ms: config.max_recovery_time * 1_000},
      {ProducerMonitor,
       producers: Producers.fetch(),
       handler: config.handler,
       inactivity_ms: config.inactivity_seconds * 1_000,
       recover: &Recovery.request/1}
    ]

    Supervisor.init(lifecycle ++ child_specs(config), strategy: :one_for_one)
  end

  @doc """
  Build the feed pipeline child spec for a resolved `config`. Exposed so the
  wiring (bindings, connection) can be inspected/tested without connecting.
  """
  @spec child_specs(Config.t()) :: [Supervisor.child_spec()]
  def child_specs(%Config{} = config) do
    [
      {Pipeline,
       name: Pipeline,
       handler: config.handler,
       queue: "",
       connection: Config.amqp_connection(config),
       bindings: Session.bindings(config.node_id),
       monitor: ProducerMonitor,
       recovery: Recovery,
       checkpoint_store: config.checkpoint_store}
    ]
  end
end
