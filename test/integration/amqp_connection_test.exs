defmodule UOF.SDK.AMQP.ConnectionIntegrationTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.AMQP.Connection
  alias UOF.SDK.Transport

  @moduletag :integration

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
    {:ok, channel} = Connection.checkout_channel(Connection)
    :ok = AMQP.Exchange.declare(channel, "uof-sdk-amqp-test", :topic)
    :ok = Connection.checkin_channel(Connection, channel)
    %{transport: transport}
  end

  test "both Broadway consumers share one connection and reconnect after it closes", %{transport: transport} do
    parent = self()

    for {kind, producer} <- [system: transport.system, content: transport.content] do
      {module, opts} = producer

      opts =
        Keyword.update!(opts, :bindings, fn bindings ->
          Enum.map(bindings, fn {_exchange, binding} -> {"uof-sdk-amqp-test", binding} end)
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

    :ok = AMQP.Basic.publish(content, "uof-sdk-amqp-test", "-.-.-.alive.-.-.-.-", "alive")
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

    :ok = AMQP.Basic.publish(new_content, "uof-sdk-amqp-test", "-.-.-.alive.-.-.-.-", "reconnected")
    assert_receive {:delivery, "reconnected", third}, 5_000
    assert_receive {:delivery, "reconnected", fourth}, 5_000
    refute third.consumer_tag in [first.consumer_tag, second.consumer_tag]
    refute fourth.consumer_tag in [first.consumer_tag, second.consumer_tag]
  end

  test "checking in one channel leaves the other usable and owner shutdown closes the socket" do
    {:ok, first} = Connection.checkout_channel(Connection)
    {:ok, second} = Connection.checkout_channel(Connection)
    assert first.conn.pid == second.conn.pid

    :ok = Connection.checkin_channel(Connection, first)
    assert {:ok, _queue} = AMQP.Queue.declare(second, "", exclusive: true)

    ref = Process.monitor(second.conn.pid)
    stop_supervised!(Connection)
    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000
  end

  test "an owner crash does not leave an orphan connection" do
    {:ok, channel} = Connection.checkout_channel(Connection)
    ref = Process.monitor(channel.conn.pid)
    Process.exit(Process.whereis(Connection), :kill)
    assert_receive {:DOWN, ^ref, :process, _, _}, 5_000

    # Synchronize with the supervisor's restart before checking out again.
    {:ok, supervisor} = ExUnit.fetch_test_supervisor()
    [{Connection, owner, :worker, _}] = Supervisor.which_children(supervisor)
    assert is_pid(owner)
    assert {:ok, replacement} = Connection.checkout_channel(owner)
    refute replacement.conn.pid == channel.conn.pid
  end

  defp rabbitmq_port do
    "RABBITMQ_AMQP_PORT" |> System.get_env("15672") |> String.to_integer()
  end
end
