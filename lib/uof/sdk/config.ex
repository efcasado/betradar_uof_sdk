defmodule UOF.SDK.Config do
  @schema [
    handler: [
      type: {:custom, __MODULE__, :validate_module, [UOF.SDK.MessageHandler]},
      required: true,
      doc: "Module implementing `UOF.SDK.MessageHandler`."
    ],
    transport: [
      type:
        {:or,
         [
           {:in, [:amqp]},
           {:tuple, [{:in, [:amqp]}, :keyword_list]},
           {:tuple, [{:in, [:pulsar]}, :keyword_list]}
         ]},
      default: :amqp,
      doc: "`{:amqp, opts}` or `{:pulsar, opts}`. The transport validates its own options."
    ],
    node_id: [
      type: {:or, [:pos_integer, {:in, [nil]}]},
      default: nil,
      doc: """
      Scopes recovery replays and `snapshot_complete`. Shared by every instance on one Pulsar
      subscription; distinct across subscriptions using the same access token.
      """
    ],
    monitor_store: [
      type: {:custom, __MODULE__, :validate_module, [UOF.SDK.ProducerMonitor.Store]},
      default: UOF.SDK.ProducerMonitor.Store.ETS,
      doc: "Module implementing `UOF.SDK.ProducerMonitor.Store`."
    ],
    concurrency: [
      type: :pos_integer,
      default: 10,
      doc: "Number of content processors in each SDK instance."
    ],
    inactivity_seconds: [
      type: :pos_integer,
      default: 20,
      doc: "Heartbeat timeout and session-readiness waiting interval."
    ],
    max_processing_delay_seconds: [
      type: :pos_integer,
      default: 20,
      doc: "Content-lag threshold and restart-resume freshness bound."
    ],
    min_interval_between_recoveries: [
      type: :pos_integer,
      default: 30,
      doc: "Per-producer recovery-request cooldown and retry delay."
    ],
    max_recovery_time: [
      type: :pos_integer,
      default: 3600,
      doc: "Wait for recovery completion before reissuing a stalled request."
    ],
    recovery_overlap_seconds: [
      type: :pos_integer,
      default: 300,
      doc: """
      Look-back subtracted from the stored checkpoint for incremental recovery. Must exceed
      `:max_processing_delay_seconds`.
      """
    ]
  ]

  @moduledoc """
  Resolves the SDK configuration from application env (plus per-call overrides)
  into a normalized struct.

  ## Options

  #{NimbleOptions.docs(@schema)}

  ## Example — AMQP transport

      config :uof_sdk,
        handler: MyApp.FeedHandler,
        node_id: 1,
        transport: {:amqp,
          connection: [
            host: "stgmq.betradar.com",
            username: System.get_env("UOF_ACCESS_TOKEN"),
            password: "",
            virtual_host: "/unifiedfeed/12345",
            ssl_options: []
          ]
        }

  ## Example — Pulsar transport

      config :uof_sdk,
        handler: MyApp.FeedHandler,
        node_id: 1,
        transport: {:pulsar,
          host: "pulsar://localhost:6650",
          topic: "uof-feed",
          subscription: "uof-sdk"
        }
  """

  alias UOF.SDK.MessageMetadata
  alias UOF.SDK.Transport

  @otp_app :uof_sdk

  @type t :: %__MODULE__{
          handler: module(),
          node_id: pos_integer() | nil,
          transport: term(),
          transport_children: [Transport.child_spec()],
          content_producer: Transport.producer_spec(),
          system_producer: Transport.producer_spec(),
          ownership: Transport.ownership(),
          metadata_adapter: MessageMetadata.adapter(),
          routing_key_metadata_key: atom(),
          connection_token_metadata_key: atom() | nil,
          monitor_store: module(),
          concurrency: pos_integer(),
          inactivity_seconds: pos_integer(),
          max_processing_delay_seconds: pos_integer(),
          min_interval_between_recoveries: pos_integer(),
          max_recovery_time: pos_integer(),
          recovery_overlap_seconds: pos_integer()
        }

  defstruct [
    :handler,
    :node_id,
    :transport,
    :transport_children,
    :content_producer,
    :system_producer,
    :ownership,
    :metadata_adapter,
    :routing_key_metadata_key,
    :connection_token_metadata_key,
    :monitor_store,
    :concurrency,
    :inactivity_seconds,
    :max_processing_delay_seconds,
    :min_interval_between_recoveries,
    :max_recovery_time,
    :recovery_overlap_seconds
  ]

  @doc """
  Load config from application env, merging `overrides` on top, and validate it.
  """
  @spec load(keyword()) :: t()
  def load(overrides \\ []) do
    cfg =
      @otp_app
      |> Application.get_all_env()
      |> Keyword.merge(overrides)
      |> NimbleOptions.validate!(@schema)
      |> validate_recovery_overlap!()

    producers = Transport.producers(cfg[:transport], cfg[:node_id])

    struct!(
      __MODULE__,
      Keyword.merge(cfg,
        transport_children: producers.children,
        content_producer: producers.content,
        system_producer: producers.system,
        ownership: producers.ownership,
        metadata_adapter: producers.metadata_adapter,
        routing_key_metadata_key: producers.routing_key_metadata_key,
        connection_token_metadata_key: producers.connection_token_metadata_key
      )
    )
  end

  # Subscribed alives advance the checkpoint while a producer is :up, which
  # tolerates content lagging by up to the processing delay. Incremental
  # recovery must look back further than that, or a restart can skip messages
  # that were delivered but not yet processed.
  defp validate_recovery_overlap!(cfg) do
    overlap = Keyword.fetch!(cfg, :recovery_overlap_seconds)
    delay = Keyword.fetch!(cfg, :max_processing_delay_seconds)

    if overlap <= delay do
      raise ArgumentError,
            "recovery_overlap_seconds (#{overlap}) must exceed max_processing_delay_seconds (#{delay})"
    end

    cfg
  end

  @doc false
  def validate_module(module, behaviour) when is_atom(module) do
    if Code.ensure_loaded?(module) do
      callbacks = behaviour.behaviour_info(:callbacks) -- behaviour.behaviour_info(:optional_callbacks)

      case Enum.reject(callbacks, fn {fun, arity} -> function_exported?(module, fun, arity) end) do
        [] -> {:ok, module}
        missing -> {:error, "#{inspect(module)} does not implement #{inspect(behaviour)} callbacks: #{inspect(missing)}"}
      end
    else
      {:error, "expected a loaded module implementing #{inspect(behaviour)}, got: #{inspect(module)}"}
    end
  end

  def validate_module(other, behaviour) do
    {:error, "expected a module implementing #{inspect(behaviour)}, got: #{inspect(other)}"}
  end
end
