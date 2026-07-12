defmodule UOF.SDK.Config do
  @moduledoc """
  Resolves the SDK configuration from application env (plus per-call overrides)
  into a normalized struct.

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
  alias UOF.SDK.ProducerMonitor.Store.ETS
  alias UOF.SDK.Transport

  @otp_app :uof_sdk

  @type t :: %__MODULE__{
          handler: module(),
          node_id: integer() | nil,
          transport: term(),
          content_producer: Transport.producer_spec(),
          system_producer: Transport.producer_spec(),
          metadata_adapter: MessageMetadata.adapter(),
          routing_key_metadata_key: atom(),
          connection_token_metadata_key: atom() | nil,
          monitor_store: module(),
          concurrency: pos_integer(),
          inactivity_seconds: pos_integer(),
          max_processing_delay_seconds: pos_integer(),
          min_interval_between_recoveries: pos_integer(),
          max_recovery_time: pos_integer(),
          recovery_overlap_seconds: non_neg_integer()
        }

  defstruct [
    :handler,
    :node_id,
    :transport,
    :content_producer,
    :system_producer,
    :connection_token_metadata_key,
    metadata_adapter: :amqp,
    routing_key_metadata_key: :routing_key,
    monitor_store: ETS,
    concurrency: 10,
    inactivity_seconds: 20,
    max_processing_delay_seconds: 20,
    min_interval_between_recoveries: 30,
    max_recovery_time: 3600,
    recovery_overlap_seconds: 300
  ]

  @doc """
  Load config from application env, merging `overrides` on top. Raises
  `ArgumentError` on a missing `:handler`.
  """
  @spec load(keyword()) :: t()
  def load(overrides \\ []) do
    cfg = Keyword.merge(Application.get_all_env(@otp_app), overrides)

    transport = Keyword.get(cfg, :transport, :amqp)
    producers = Transport.producers(transport, Keyword.get(cfg, :node_id))

    %__MODULE__{
      handler: fetch!(cfg, :handler),
      node_id: Keyword.get(cfg, :node_id),
      transport: transport,
      content_producer: producers.content,
      system_producer: producers.system,
      metadata_adapter: producers.metadata_adapter,
      routing_key_metadata_key: producers.routing_key_metadata_key,
      connection_token_metadata_key: producers.connection_token_metadata_key,
      monitor_store: Keyword.get(cfg, :monitor_store, ETS),
      concurrency: Keyword.get(cfg, :concurrency, 10),
      inactivity_seconds: Keyword.get(cfg, :inactivity_seconds, 20),
      max_processing_delay_seconds: Keyword.get(cfg, :max_processing_delay_seconds, 20),
      min_interval_between_recoveries: Keyword.get(cfg, :min_interval_between_recoveries, 30),
      max_recovery_time: Keyword.get(cfg, :max_recovery_time, 3600),
      recovery_overlap_seconds: Keyword.get(cfg, :recovery_overlap_seconds, 300)
    }
  end

  defp fetch!(cfg, key) do
    case Keyword.fetch(cfg, key) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "missing required config :#{key} for #{@otp_app}"
    end
  end
end
