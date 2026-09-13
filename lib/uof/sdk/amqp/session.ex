defmodule UOF.SDK.AMQP.Session do
  @moduledoc false

  # Deliberately monitored, not linked to the SDK owner. AMQP.Connection.open/1 creates its
  # connection under amqp_sup; killing the caller mid-handshake cannot cancel it.
  # This session finishes the bounded open and closes its result if the owner
  # has gone away. Its registered name excludes another attempt until cleanup
  # completes. The connection is never handed off to another owning process.
  use GenServer

  alias AMQP.Connection

  @compile {:no_warn_undefined, Connection}

  def start(owner, options) do
    GenServer.start(__MODULE__, {owner, options}, name: __MODULE__)
  end

  @impl true
  def init({owner, options}) do
    Process.flag(:trap_exit, true)
    ref = Process.monitor(owner)
    {:ok, %{owner: owner, owner_ref: ref, options: options, connection: nil}, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    if Process.alive?(state.owner), do: connect(state), else: {:stop, :normal, state}
  end

  defp connect(state) do
    case open(state.options) do
      {:ok, connection} ->
        Process.link(connection.pid)
        state = %{state | connection: connection}

        if Process.alive?(state.owner) do
          send(state.owner, {:connected, self(), connection})
          {:noreply, state}
        else
          {:stop, :normal, state}
        end

      {:error, reason} ->
        send(state.owner, {:connect_failed, self(), reason})
        {:stop, :normal, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _, _}, %{owner_ref: ref} = state), do: {:stop, :normal, state}
  def handle_info({:EXIT, pid, _}, %{connection: %{pid: pid}} = state), do: {:stop, :normal, state}

  @impl true
  def terminate(_, %{connection: nil}), do: :ok

  def terminate(_, %{connection: connection}) do
    # Waiting for DOWN keeps the session name reserved until the old socket is
    # gone. Prefer the AMQP close handshake, but guarantee disposal if it fails.
    ref = Process.monitor(connection.pid)

    try do
      Connection.close(connection)
    catch
      :exit, _ -> :ok
    end

    receive do
      {:DOWN, ^ref, :process, _, _} -> :ok
    after
      1_000 ->
        Process.exit(connection.pid, :kill)

        receive do
          {:DOWN, ^ref, :process, _, _} -> :ok
        end
    end
  end

  defp open(options) do
    metadata = %{connection: options, connection_name: :undefined}

    :telemetry.span([:broadway_rabbitmq, :amqp, :open_connection], metadata, fn ->
      result = do_open(options)
      {result, Map.put(metadata, :result, result)}
    end)
  end

  defp do_open(options) do
    Connection.open(options)
  catch
    :exit, {:timeout, {:gen_server, :call, [pid, :connect, timeout]}} when is_integer(timeout) ->
      # The AMQP call can time out before its independently supervised process.
      # Ensure that process cannot complete a handshake after this session ends.
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^ref, :process, _, _} -> :ok
      end

      {:error, :timeout}
  end
end
