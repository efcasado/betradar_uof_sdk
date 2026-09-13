defmodule UOF.SDK.AMQP.Client do
  @moduledoc false

  # BroadwayRabbitMQ 0.8.2 expects pool failures as exceptions, but its producer
  # only backs off for selected AMQP reasons. This adapter bridges that contract
  # for Connection and owns channel setup so failures release acquired channels.
  # Known transient failures use Broadway's backoff; unexpected exits and
  # exceptions propagate after cleanup. Delivery operations stay with upstream.
  # Revisit this adapter when upgrading BroadwayRabbitMQ's private client API.

  alias AMQP.Basic
  alias AMQP.Queue
  alias BroadwayRabbitMQ.AmqpClient
  alias UOF.SDK.AMQP.Error

  require Logger

  @rabbitmq_client if(Code.ensure_loaded?(BroadwayRabbitMQ.RabbitmqClient),
                     do: BroadwayRabbitMQ.RabbitmqClient,
                     else: false
                   )
  if @rabbitmq_client, do: @behaviour(@rabbitmq_client)

  @compile {:no_warn_undefined, [Basic, Queue, AmqpClient]}

  @impl @rabbitmq_client
  def init(opts) do
    {{:custom_pool, _module, _args} = pool, opts} = Keyword.pop!(opts, :connection)

    # Validate producer options independently of the internal pool. Its callback
    # functions exist even if this SDK was compiled before the optional adapter;
    # runtime operation must not depend on compile-time behaviour attributes.
    with {:ok, config} <- AmqpClient.init(opts), do: {:ok, %{config | connection: pool}}
  end

  @impl @rabbitmq_client
  defdelegate ack(channel, delivery_tag), to: AmqpClient
  @impl @rabbitmq_client
  defdelegate reject(channel, delivery_tag, opts), to: AmqpClient
  @impl @rabbitmq_client
  defdelegate consume(channel, config), to: AmqpClient
  @impl @rabbitmq_client
  defdelegate cancel(channel, consumer_tag), to: AmqpClient
  @impl @rabbitmq_client
  defdelegate close_connection(config, channel), to: AmqpClient

  @impl @rabbitmq_client
  def setup_channel(%{connection: {:custom_pool, pool, args}} = config) do
    case pool.checkout_channel(args) do
      {:ok, channel} -> setup(channel, config, pool, args)
      {:error, %Error{} = error} -> failure(error)
    end
  end

  # Keep the acquired channel in scope for every failure path, including an
  # exit between checkout and queue binding. Delegate validation and delivery
  # operations to BroadwayRabbitMQ; own setup so cleanup is unconditional.
  defp setup(channel, config, pool, args) do
    result =
      try do
        Process.link(channel.pid)

        with :ok <- config.after_connect.(channel),
             :ok <- Basic.qos(channel, config.qos),
             {:ok, queue} <- declare(channel, config),
             :ok <- bind(channel, queue, config.bindings) do
          {:ok, channel}
        end
      rescue
        exception ->
          pool.checkin_channel(args, channel)
          reraise exception, __STACKTRACE__
      catch
        :exit, reason ->
          if Error.retryable?(reason) do
            {:error, reason}
          else
            pool.checkin_channel(args, channel)
            :erlang.raise(:exit, reason, __STACKTRACE__)
          end
      end

    case result do
      {:ok, _} ->
        result

      {:error, reason} ->
        pool.checkin_channel(args, channel)
        failure(%Error{operation: :setup, reason: reason})

      unexpected ->
        pool.checkin_channel(args, channel)
        raise ArgumentError, "unexpected AMQP setup result: #{inspect(unexpected)}"
    end
  end

  defp declare(_, %{declare_opts: nil, queue: queue}), do: {:ok, queue}

  defp declare(channel, config) do
    case Queue.declare(channel, config.queue, config.declare_opts) do
      {:ok, %{queue: queue}} -> {:ok, queue}
      error -> error
    end
  end

  defp bind(_, _, []), do: :ok

  defp bind(channel, queue, [{exchange, options} | rest]) do
    with :ok <- Queue.bind(channel, queue, exchange, options), do: bind(channel, queue, rest)
  end

  defp failure(%Error{} = error) do
    retryable = Error.retryable?(error)

    :telemetry.execute([:uof_sdk, :amqp, :setup_failure], %{system_time: System.system_time()}, %{
      operation: error.operation,
      reason: error.reason,
      retryable: retryable
    })

    # Broadway already logs :econnrefused verbatim. Other transient reasons
    # need their original cause logged before translation to its backoff alias.
    if error.reason not in [:connecting, :econnrefused] do
      Logger.log(if(retryable, do: :warning, else: :error), Exception.message(error))
    end

    # BroadwayRabbitMQ 0.8 only retries selected reasons. The SDK event above
    # retains the original reason; only retryable failures use its backoff alias.
    # Broadway retries :not_allowed itself, so returning it would override our
    # permanent permission-failure policy.
    if error.reason == :not_allowed, do: raise(error)

    {:error, if(retryable, do: :econnrefused, else: error.reason)}
  end
end
