defmodule UOF.SDK.AMQP.Connection do
  @moduledoc false

  # Betradar permits one connection, while BroadwayRabbitMQ's default client
  # opens a connection per producer. Its ChannelPool extension lets both SDK
  # pipelines share one connection and obtain separate channels.
  # This supervised coordinator delegates socket ownership to Session so
  # checkouts can return promptly while a connection attempt is in progress.

  use GenServer

  alias AMQP.Channel
  alias BroadwayRabbitMQ.AmqpClient
  alias UOF.SDK.AMQP.Error
  alias UOF.SDK.AMQP.Session

  @channel_pool if(Code.ensure_loaded?(BroadwayRabbitMQ.ChannelPool), do: BroadwayRabbitMQ.ChannelPool, else: false)
  if @channel_pool, do: @behaviour(@channel_pool)

  @compile {:no_warn_undefined, [Channel, AmqpClient]}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, validate!(Keyword.fetch!(opts, :connection)), name: __MODULE__)
  end

  # Reuse the adapter's validation before replacing connection options with a
  # custom pool. This preserves URI and unknown-key checks without a second schema.
  def validate!(options) when is_binary(options) or is_list(options) do
    case AmqpClient.init(queue: "", declare: [exclusive: true], connection: options) do
      {:ok, _} -> options
      {:error, reason} -> raise ArgumentError, "invalid AMQP connection: #{reason}"
    end
  end

  def validate!(other) do
    raise ArgumentError, "expected AMQP :connection to be a keyword list or a URI, got: #{inspect(other)}"
  end

  @impl true
  def init(options), do: {:ok, %{options: options, session: nil, connection: nil, error: nil}}

  @impl @channel_pool
  def checkout_channel(server) do
    with {:ok, connection} <- GenServer.call(server, :connection) do
      # Open in the producer process so SelectiveConsumer belongs to it.
      case Channel.open(connection) do
        {:ok, channel} -> {:ok, channel}
        {:error, reason} -> {:error, %Error{operation: :checkout, reason: reason}}
      end
    end
  catch
    :exit, reason ->
      if Error.retryable?(reason) do
        {:error, %Error{operation: :checkout, reason: reason}}
      else
        :erlang.raise(:exit, reason, __STACKTRACE__)
      end
  end

  @impl @channel_pool
  # AMQP may reply before teardown completes, or report an already-closing
  # channel. Leave that teardown to the protocol: force-killing a channel is
  # reported as an internal error and would close the shared connection.
  def checkin_channel(_server, channel) do
    Channel.close(channel)
    :ok
  catch
    :exit, _ -> :ok
  end

  @impl true
  def handle_call(:connection, _, %{connection: %{pid: pid} = connection} = state) do
    if Process.alive?(pid) do
      {:reply, {:ok, connection}, state}
    else
      connect(%{state | connection: nil})
    end
  end

  def handle_call(:connection, _, state), do: connect(state)

  @impl true
  def handle_info({:connected, pid, connection}, %{session: {pid, _}} = state) do
    {:noreply, %{state | connection: connection, error: nil}}
  end

  def handle_info({:connect_failed, pid, reason}, %{session: {pid, _}} = state) do
    {:noreply, %{state | error: reason}}
  end

  def handle_info({:DOWN, ref, :process, _, :normal}, %{session: {_, ref}} = state) do
    {:noreply, %{state | session: nil, connection: nil}}
  end

  def handle_info({:DOWN, ref, :process, _, reason}, %{session: {_, ref}} = state) do
    {:stop, reason, state}
  end

  defp connect(state) do
    reason = state.error || :connecting
    state = if Error.retryable?(reason), do: ensure_session(%{state | error: nil}), else: state

    # Report the previous failure while starting the next transient attempt.
    # A new startup error takes precedence; permanent errors remain visible.
    {:reply, {:error, %Error{operation: :connect, reason: state.error || reason}}, state}
  end

  defp ensure_session(%{session: nil} = state) do
    case Session.start(self(), state.options) do
      {:ok, session} ->
        %{state | session: session}

      {:error, {:already_started, _}} ->
        # A previous owner's session is still finishing its handshake/cleanup.
        state

      {:error, reason} ->
        %{state | error: reason}
    end
  end

  defp ensure_session(state), do: state
end
