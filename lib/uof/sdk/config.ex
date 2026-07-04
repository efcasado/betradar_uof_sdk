defmodule UOF.SDK.Config do
  @moduledoc """
  Resolves the SDK configuration from application env (plus per-call overrides)
  into a normalized struct.

  ## Example — built-in AMQP producer

      config :uof_sdk,
        handler: MyApp.FeedHandler,
        node_id: 1,
        connection: [
          host: "stgmq.betradar.com",
          username: System.get_env("UOF_ACCESS_TOKEN"),
          password: "",
          virtual_host: "/unifiedfeed/12345",
          ssl_options: []
        ]

  ## Example — custom producer (e.g. Pulsar)

      config :uof_sdk,
        handler: MyApp.FeedHandler,
        node_id: 1,
        producer: {MyPulsarProducer, topic: "uof-feed"},
        routing_key_metadata_key: :pulsar_key

  `:connection` is passed directly to `BroadwayRabbitMQ.Producer` and is
  ignored when `:producer` is set. No fields are derived or defaulted — what
  you pass is exactly what the broker sees.
  """

  alias UOF.SDK.CheckpointStore.ETS

  @otp_app :uof_sdk

  @type t :: %__MODULE__{
          handler: module(),
          node_id: integer() | nil,
          producer: {module(), keyword()} | module() | nil,
          connection: keyword(),
          routing_key_metadata_key: atom(),
          connection_token_metadata_key: atom() | nil,
          checkpoint_store: module(),
          inactivity_seconds: pos_integer(),
          min_interval_between_recoveries: pos_integer(),
          max_recovery_time: pos_integer()
        }

  defstruct [
    :handler,
    :node_id,
    :producer,
    :connection_token_metadata_key,
    connection: [],
    routing_key_metadata_key: :routing_key,
    checkpoint_store: ETS,
    inactivity_seconds: 20,
    min_interval_between_recoveries: 30,
    max_recovery_time: 3600
  ]

  @doc """
  Load config from application env, merging `overrides` on top. Raises
  `ArgumentError` on a missing `:handler`.
  """
  @spec load(keyword()) :: t()
  def load(overrides \\ []) do
    cfg = Keyword.merge(Application.get_all_env(@otp_app), overrides)

    %__MODULE__{
      handler: fetch!(cfg, :handler),
      node_id: Keyword.get(cfg, :node_id),
      producer: Keyword.get(cfg, :producer),
      connection: Keyword.get(cfg, :connection, []),
      routing_key_metadata_key: Keyword.get(cfg, :routing_key_metadata_key, :routing_key),
      connection_token_metadata_key: Keyword.get(cfg, :connection_token_metadata_key),
      checkpoint_store: Keyword.get(cfg, :checkpoint_store, ETS),
      inactivity_seconds: Keyword.get(cfg, :inactivity_seconds, 20),
      min_interval_between_recoveries: Keyword.get(cfg, :min_interval_between_recoveries, 30),
      max_recovery_time: Keyword.get(cfg, :max_recovery_time, 3600)
    }
  end

  defp fetch!(cfg, key) do
    case Keyword.fetch(cfg, key) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "missing required config :#{key} for #{@otp_app}"
    end
  end
end
