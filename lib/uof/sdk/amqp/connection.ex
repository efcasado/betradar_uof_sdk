if Code.ensure_loaded?(BroadwayRabbitMQ.ChannelPool) do
  defmodule UOF.SDK.AMQP.Connection do
    @moduledoc false

    @behaviour BroadwayRabbitMQ.ChannelPool

    use GenServer

    alias AMQP.Channel
    alias AMQP.Connection

    # Both producers check out channels from this single connection owner.
    # Connection attempts are serialized here; Broadway owns retry/backoff.
    def start_link(opts) do
      GenServer.start_link(__MODULE__, Keyword.fetch!(opts, :connection), name: Keyword.get(opts, :name, __MODULE__))
    end

    @impl GenServer
    def init(options) do
      Process.flag(:trap_exit, true)
      {:ok, %{options: options, connection: nil}}
    end

    @impl BroadwayRabbitMQ.ChannelPool
    def checkout_channel(server) do
      # The AMQP client's connection timeout bounds the attempt. A shorter
      # call timeout could leave a successful connection behind the caller.
      with {:ok, connection} <- GenServer.call(server, :connection, :infinity) do
        # Open in the producer process so SelectiveConsumer belongs to it.
        case Channel.open(connection) do
          {:ok, channel} -> {:ok, channel}
          {:error, reason} -> pool_error(reason)
        end
      end
    catch
      :exit, reason -> pool_error(reason)
    end

    @impl BroadwayRabbitMQ.ChannelPool
    def checkin_channel(_server, channel) do
      # Never close the shared connection when a producer stops or its queue
      # setup fails. Channels are disposed of rather than reused.
      case Channel.close(channel) do
        :ok -> :ok
        {:error, reason} -> pool_error(reason)
      end
    catch
      :exit, {:noproc, _} -> :ok
      :exit, reason -> pool_error(reason)
    end

    @impl GenServer
    def handle_call(:connection, _from, state) do
      if state.connection && Process.alive?(state.connection.pid) do
        {:reply, {:ok, state.connection}, state}
      else
        case open(state.options) do
          {:ok, connection} ->
            Process.link(connection.pid)
            {:reply, {:ok, connection}, %{state | connection: connection}}

          {:error, reason} ->
            {:reply, pool_error(reason), %{state | connection: nil}}
        end
      end
    end

    @impl GenServer
    def handle_info({:EXIT, pid, _reason}, %{connection: %{pid: pid}} = state) do
      {:noreply, %{state | connection: nil}}
    end

    # An old connection's EXIT can arrive after a checkout has replaced it.
    def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

    @impl GenServer
    def terminate(_reason, %{connection: nil}), do: :ok

    def terminate(_reason, %{connection: connection}) do
      if Process.alive?(connection.pid), do: Connection.close(connection)
    catch
      :exit, _reason -> :ok
    end

    defp open(options) do
      Connection.open(options)
    catch
      # Mirror BroadwayRabbitMQ's cleanup of a timed-out connect call: the
      # underlying AMQP process must not establish an orphan connection later.
      :exit, {:timeout, {:gen_server, :call, [pid, :connect, timeout]}} when is_integer(timeout) ->
        Process.exit(pid, :kill)
        {:error, :timeout}
    end

    defp pool_error(reason), do: {:error, RuntimeError.exception("AMQP channel checkout failed: #{inspect(reason)}")}
  end
end
