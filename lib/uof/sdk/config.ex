defmodule UOF.SDK.Config do
  @schema [
    handler: [
      type: {:custom, __MODULE__, :validate_module, [UOF.SDK.MessageHandler]},
      required: true,
      doc: "Module implementing `UOF.SDK.MessageHandler`."
    ],
    pulsar: [
      type: :keyword_list,
      required: true,
      doc: """
      Pulsar connection and subscription settings. `:host`, `:topic`, and `:subscription` are
      required; `:conn_opts` configures the shared client, and other options are passed to
      `OffBroadway.Pulsar.Producer`.
      """
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
      type: {:custom, __MODULE__, :validate_store, []},
      default: UOF.SDK.ProducerMonitor.Store.ETS,
      doc: """
      `module` or `{module, opts}` implementing `UOF.SDK.ProducerMonitor.Store`. `opts` is
      passed to its callbacks, for example to select a key per monitor.
      """
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
    producers: [
      type:
        {:list,
         {:keyword_list,
          [
            id: [type: :pos_integer, required: true],
            product: [type: :string, required: true, doc: "Recovery API path segment, e.g. `\"liveodds\"`."],
            name: [type: {:or, [:string, nil]}, default: nil],
            recovery_window_minutes: [type: {:or, [:pos_integer, nil]}, default: nil]
          ]}},
      default: [],
      doc: """
      Producer descriptions used only when the UOF API cannot provide them at startup and
      none were stored by a previous run. Each entry needs `:id` and `:product`.
      """
    ],
    recovery_overlap_seconds: [
      type: :pos_integer,
      default: 300,
      doc: """
      Look-back subtracted from the stored checkpoint for incremental recovery.
      """
    ]
  ]

  @moduledoc """
  Resolves the SDK configuration from application env (plus per-call overrides)
  into a normalized struct.

  ## Options

  #{NimbleOptions.docs(@schema)}

  ## Example

      config :uof_sdk,
        handler: MyApp.FeedHandler,
        node_id: 1,
        pulsar: [
          host: "pulsar://localhost:6650",
          topic: "uof-feed",
          subscription: "uof-sdk"
        ]
  """

  alias UOF.SDK.Transport

  @otp_app :uof_sdk

  @type t :: %__MODULE__{
          handler: module(),
          node_id: pos_integer() | nil,
          pulsar: keyword(),
          transport_children: [Transport.child_spec()],
          content_producer: Transport.producer_spec(),
          system_producer: Transport.producer_spec(),
          monitor_store: {module(), keyword()},
          concurrency: pos_integer(),
          inactivity_seconds: pos_integer(),
          min_interval_between_recoveries: pos_integer(),
          max_recovery_time: pos_integer(),
          recovery_overlap_seconds: pos_integer(),
          producers: [map()]
        }

  defstruct [
    :handler,
    :node_id,
    :pulsar,
    :transport_children,
    :content_producer,
    :system_producer,
    :monitor_store,
    :concurrency,
    :inactivity_seconds,
    :min_interval_between_recoveries,
    :max_recovery_time,
    :recovery_overlap_seconds,
    :producers
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

    pipelines = Transport.producers(cfg[:pulsar])

    struct!(
      __MODULE__,
      Keyword.merge(cfg,
        producers: Enum.map(cfg[:producers], &Map.new/1),
        transport_children: pipelines.children,
        content_producer: pipelines.content,
        system_producer: pipelines.system
      )
    )
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

  @doc false
  def validate_store({module, opts}) when is_list(opts) do
    if Keyword.keyword?(opts) do
      with {:ok, module} <- validate_module(module, UOF.SDK.ProducerMonitor.Store), do: {:ok, {module, opts}}
    else
      {:error, "expected store options to be a keyword list, got: #{inspect(opts)}"}
    end
  end

  def validate_store(module), do: validate_store({module, []})
end
