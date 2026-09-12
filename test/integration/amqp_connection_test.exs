defmodule UOF.SDK.AMQP.ConnectionIntegrationTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.AMQP.Client
  alias UOF.SDK.AMQP.Connection
  alias UOF.SDK.AMQP.Error
  alias UOF.SDK.Transport

  @moduletag :integration

  @exchange "uof-sdk-amqp-test"

  defmodule Probe do
    @moduledoc false
    use Broadway

    def start_link(opts) do
      Broadway.start_link(__MODULE__,
        name: Keyword.fetch!(opts, :name),
        producer: [module: Keyword.fetch!(opts, :producer), concurrency: 1],
        processors: [default: [concurrency: 1]],
        context: Keyword.fetch!(opts, :test_pid)
      )
    end

    @impl true
    def handle_message(_, message, test_pid) do
      send(test_pid, {:delivery, message.data, message.metadata})
      message
    end
  end

  setup do
    transport =
      Transport.producers(
        {:amqp, connection: [host: "localhost", port: rabbitmq_port()]},
        nil
      )

    [owner] = transport.children
    start_supervised!(owner)
    %{transport: transport}
  end

  test "both Broadway consumers share one connection and reconnect after it closes", %{transport: transport} do
    {:ok, channel} = await_channel(Connection)
    :ok = AMQP.Exchange.declare(channel, @exchange, :topic)
    :ok = Connection.checkin_channel(Connection, channel)

    on_exit(fn ->
      if session = Process.whereis(UOF.SDK.AMQP.Session) do
        ref = Process.monitor(session)
        assert_receive {:DOWN, ^ref, :process, _, _}, 5_000
      end

      {:ok, conn} = AMQP.Connection.open(host: "localhost", port: rabbitmq_port())

      try do
        {:ok, chan} = AMQP.Channel.open(conn)
        :ok = AMQP.Exchange.delete(chan, @exchange)
      after
        AMQP.Connection.close(conn)
      end
    end)

    parent = self()

    for {kind, producer} <- [system: transport.system, content: transport.content] do
      {module, opts} = producer

      opts =
        Keyword.update!(opts, :bindings, fn bindings ->
          Enum.map(bindings, fn {_exchange, binding} -> {@exchange, binding} end)
        end)

      opts =
        Keyword.put(opts, :after_connect, fn channel ->
          send(parent, {:channel, kind, channel})
          :ok
        end)

      start_supervised!(%{
        id: kind,
        start: {Probe, :start_link, [[name: Module.concat(__MODULE__, kind), producer: {module, opts}, test_pid: self()]]}
      })
    end

    assert_receive {:channel, :system, system}, 5_000
    assert_receive {:channel, :content, content}, 5_000
    assert system.conn.pid == content.conn.pid
    refute system.pid == content.pid

    # These calls also act as barriers after each producer has subscribed.
    for kind <- [:system, :content] do
      name = Module.concat(__MODULE__, kind)
      for producer <- Broadway.producer_names(name), do: :sys.get_state(producer)
    end

    :ok = AMQP.Basic.publish(content, @exchange, "-.-.-.alive.-.-.-.-", "alive")
    assert_receive {:delivery, "alive", first}, 5_000
    assert_receive {:delivery, "alive", second}, 5_000
    refute first.consumer_tag == second.consumer_tag
    assert first.amqp_channel.conn.pid == second.amqp_channel.conn.pid

    # A single channel failure must leave the other consumer/socket intact.
    :ok = AMQP.Channel.close(content)
    assert_receive {:channel, :content, replacement}, 5_000
    assert replacement.conn.pid == system.conn.pid
    assert Process.alive?(system.pid)

    # A socket failure must replace both channels on one new connection.
    :ok = AMQP.Connection.close(system.conn)
    assert_receive {:channel, :system, new_system}, 5_000
    assert_receive {:channel, :content, new_content}, 5_000
    assert new_system.conn.pid == new_content.conn.pid
    refute new_system.conn.pid == system.conn.pid

    for kind <- [:system, :content] do
      for producer <- Broadway.producer_names(Module.concat(__MODULE__, kind)), do: :sys.get_state(producer)
    end

    :ok = AMQP.Basic.publish(new_content, @exchange, "-.-.-.alive.-.-.-.-", "reconnected")
    assert_receive {:delivery, "reconnected", third}, 5_000
    assert_receive {:delivery, "reconnected", fourth}, 5_000
    refute third.consumer_tag in [first.consumer_tag, second.consumer_tag]
    refute fourth.consumer_tag in [first.consumer_tag, second.consumer_tag]
  end

  test "checking in one channel leaves the other usable and owner shutdown closes the socket" do
    {:ok, first} = await_channel(Connection)
    {:ok, second} = await_channel(Connection)
    assert first.conn.pid == second.conn.pid

    :ok = Connection.checkin_channel(Connection, first)
    assert {:ok, _queue} = AMQP.Queue.declare(second, "", exclusive: true)

    ref = Process.monitor(second.conn.pid)
    stop_supervised!(Connection)
    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000
  end

  test "an owner crash does not leave an orphan connection" do
    {:ok, channel} = await_channel(Connection)
    owner = Process.whereis(Connection)
    connection_ref = Process.monitor(channel.conn.pid)
    owner_ref = Process.monitor(owner)

    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^connection_ref, :process, _, _}, 5_000
    assert_receive {:DOWN, ^owner_ref, :process, _, _}, 5_000

    # `Supervisor.which_children/1` can still report `:restarting` here, so wait
    # for the registered name to point at a new pid instead.
    replacement_owner = await_restart(owner)
    assert {:ok, replacement} = await_channel(replacement_owner)
    refute replacement.conn.pid == channel.conn.pid
  end

  test "setup failures dispose of the acquired channel and preserve the error policy", %{transport: transport} do
    Process.flag(:trap_exit, true)
    {:ok, sibling} = await_channel(Connection)
    {_, opts} = transport.content
    test_pid = self()

    for {reason, expected} <- [
          {{:shutdown, {:server_initiated_close, 320, "forced"}}, :econnrefused},
          {{:server_initiated_close, 404, "missing exchange"}, {:server_initiated_close, 404, "missing exchange"}},
          {:callback_bug, :callback_bug}
        ] do
      for kind <- [:exit, :return] do
        opts =
          Keyword.put(opts, :after_connect, fn channel ->
            send(test_pid, {:acquired, channel})
            if kind == :exit, do: exit(reason), else: {:error, reason}
          end)

        {:ok, config} = Client.init(Keyword.drop(opts, [:client, :on_failure]))

        if kind == :exit and not Error.retryable?(reason) do
          assert catch_exit(Client.setup_channel(config)) == reason
        else
          assert {:error, ^expected} = Client.setup_channel(config)
        end

        assert_receive {:acquired, channel}
        ref = Process.monitor(channel.pid)
        assert_receive {:DOWN, ^ref, :process, _, _}, 2_000
        assert Process.alive?(sibling.conn.pid)
        assert Process.alive?(sibling.pid)
      end
    end
  end

  test "a broker binding rejection is surfaced and cleanup does not double-close", %{transport: transport} do
    Process.flag(:trap_exit, true)
    {:ok, sibling} = await_channel(Connection)
    {_, opts} = transport.content
    test_pid = self()

    opts =
      opts
      |> Keyword.put(:bindings, [{"uof-sdk-missing-#{System.unique_integer([:positive])}", [routing_key: "#"]}])
      |> Keyword.put(:after_connect, fn channel ->
        send(test_pid, {:acquired, channel})
        :ok
      end)

    {:ok, config} = Client.init(Keyword.drop(opts, [:client, :on_failure]))
    reason = catch_exit(Client.setup_channel(config))
    assert {{:shutdown, {:server_initiated_close, 404, _}}, {:gen_server, :call, _}} = reason
    refute Error.retryable?(reason)
    assert_receive {:acquired, channel}
    ref = Process.monitor(channel.pid)
    assert_receive {:DOWN, ^ref, :process, _, _}, 2_000
    assert Process.alive?(sibling.conn.pid)
  end

  defp await_restart(previous, attempts \\ 100) do
    case Process.whereis(Connection) do
      pid when is_pid(pid) and pid != previous ->
        pid

      _other when attempts > 0 ->
        Process.sleep(50)
        await_restart(previous, attempts - 1)

      _other ->
        flunk("the connection owner was not restarted")
    end
  end

  defp await_channel(server, attempts \\ 100) do
    case Connection.checkout_channel(server) do
      {:ok, channel} ->
        {:ok, channel}

      {:error, %Error{reason: :connecting}} when attempts > 0 ->
        Process.sleep(50)
        await_channel(server, attempts - 1)

      other ->
        flunk("channel did not become ready: #{inspect(other)}")
    end
  end

  defp rabbitmq_port do
    "RABBITMQ_AMQP_PORT" |> System.get_env("15672") |> String.to_integer()
  end
end
