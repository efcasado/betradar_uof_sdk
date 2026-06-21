defmodule UOF.SDK.Config do
  @moduledoc """
  Resolves the SDK configuration from application env (plus per-call overrides)
  into a normalized struct.

  ## Example

      config :betradar_uof_sdk,
        handler: MyApp.FeedHandler,
        access_token: System.get_env("UOF_ACCESS_TOKEN"),
        host: "stgmq.betradar.com"   # integration; production is mq.betradar.com

  The AMQP endpoint is explicit: `:host` is required, `:port` defaults to `5671`
  and `:ssl` to `true`. Raw `BroadwayRabbitMQ` connection options (e.g.
  `ssl_options`) can be passed under `:amqp` and win over the above.
  `:virtual_host` is derived from the bookmaker id (`UOF.API.Users.whoami/0`)
  when not set explicitly.

  Known Betradar AMQP hosts: `mq.betradar.com` (production),
  `stgmq.betradar.com` (integration), `replaymq.betradar.com` (replay).
  """

  @otp_app :betradar_uof_sdk

  @type t :: %__MODULE__{
          handler: module(),
          access_token: String.t(),
          host: String.t(),
          port: :inet.port_number(),
          ssl: boolean(),
          amqp: keyword(),
          virtual_host: String.t() | nil,
          node_id: integer() | nil,
          checkpoint_store: module(),
          inactivity_seconds: pos_integer(),
          min_interval_between_recoveries: pos_integer(),
          max_recovery_time: pos_integer()
        }

  defstruct [
    :handler,
    :access_token,
    :host,
    :virtual_host,
    :node_id,
    port: 5671,
    ssl: true,
    amqp: [],
    checkpoint_store: UOF.SDK.CheckpointStore.ETS,
    inactivity_seconds: 20,
    min_interval_between_recoveries: 30,
    max_recovery_time: 3600
  ]

  @doc """
  Load config from application env, merging `overrides` on top. Raises
  `ArgumentError` on a missing required key.
  """
  @spec load(keyword()) :: t()
  def load(overrides \\ []) do
    cfg = Keyword.merge(Application.get_all_env(@otp_app), overrides)

    %__MODULE__{
      handler: fetch!(cfg, :handler),
      access_token: fetch!(cfg, :access_token),
      host: fetch!(cfg, :host),
      port: Keyword.get(cfg, :port, 5671),
      ssl: Keyword.get(cfg, :ssl, true),
      amqp: Keyword.get(cfg, :amqp, []),
      virtual_host: Keyword.get(cfg, :virtual_host),
      node_id: Keyword.get(cfg, :node_id),
      checkpoint_store: Keyword.get(cfg, :checkpoint_store, UOF.SDK.CheckpointStore.ETS),
      inactivity_seconds: Keyword.get(cfg, :inactivity_seconds, 20),
      min_interval_between_recoveries: Keyword.get(cfg, :min_interval_between_recoveries, 30),
      max_recovery_time: Keyword.get(cfg, :max_recovery_time, 3600)
    }
  end

  @doc """
  The `BroadwayRabbitMQ.Producer` `:connection` keyword for this config: host /
  port from the endpoint, the access token as the username, a blank password,
  and the virtual host when known. TLS uses `ssl_options` when `ssl: true`.

  The blank password is required: UOF authenticates on the token alone, but the
  `amqp` library otherwise defaults the password to `"guest"`. Anything passed
  under `:amqp` (e.g. `ssl_options`, an explicit `password`) wins.
  """
  @spec amqp_connection(t()) :: keyword()
  def amqp_connection(%__MODULE__{} = config) do
    [host: config.host, port: config.port]
    |> Keyword.merge(config.amqp)
    |> Keyword.put_new(:username, config.access_token)
    |> Keyword.put_new(:password, "")
    |> put_unless_nil(:virtual_host, config.virtual_host)
    |> maybe_tls(config.ssl)
  end

  defp maybe_tls(opts, true), do: Keyword.put_new(opts, :ssl_options, [])
  defp maybe_tls(opts, _false), do: opts

  defp put_unless_nil(opts, _key, nil), do: opts
  defp put_unless_nil(opts, key, value), do: Keyword.put(opts, key, value)

  defp fetch!(cfg, key) do
    case Keyword.fetch(cfg, key) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "missing required config :#{key} for #{@otp_app}"
    end
  end
end
