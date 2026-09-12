defmodule UOF.SDK.AMQP.ConnectionTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.AMQP.Connection
  alias UOF.SDK.AMQP.Error
  alias UOF.SDK.AMQP.Session
  alias UOF.SDK.TestSupport.AMQPHandshake

  # `AMQP.Connection.open/1` ignores keys it does not know and falls back to
  # guest@localhost, so these have to be rejected here: nothing downstream
  # looks at them any more.
  test "rejects unknown connection options" do
    assert_raise ArgumentError, ~r/unknown options \[:hostname, :user\]/, fn ->
      Connection.start_link(connection: [hostname: "stgmq.betradar.com", user: "token"])
    end
  end

  test "rejects a malformed connection URI" do
    assert_raise ArgumentError, ~r/failed parsing AMQP URI/, fn ->
      Connection.start_link(connection: "stgmq.betradar.com")
    end
  end

  test "rejects a connection that is neither a keyword list nor a URI" do
    assert_raise ArgumentError, ~r/expected AMQP :connection to be a keyword list or a URI/, fn ->
      Connection.start_link(connection: %{host: "stgmq.betradar.com"})
    end
  end

  for stop <- [:shutdown, :kill] do
    @stop stop
    test "#{stop} during a handshake keeps cleanup ownership and excludes a replacement attempt" do
      test_pid = self()
      id = make_ref()
      prefix = [:broadway_rabbitmq, :amqp, :open_connection]

      :ok =
        :telemetry.attach_many(
          id,
          [prefix ++ [:start], prefix ++ [:stop]],
          fn event, measurements, _, _ ->
            send(test_pid, {:connection_event, List.last(event), measurements})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(id) end)
      {broker, port} = AMQPHandshake.start(self())
      options = [host: "127.0.0.1", port: port]
      owner = start_supervised!({Connection, connection: options}, restart: :temporary)
      assert {:error, %Error{reason: :connecting}} = Connection.checkout_channel(owner)
      assert_receive {:handshake_waiting, server}, 2_000
      assert_receive {:connection_event, :start, %{system_time: _}}
      session = Process.whereis(Session)
      session_ref = Process.monitor(session)
      owner_ref = Process.monitor(owner)

      if @stop == :kill, do: Process.exit(owner, :kill), else: stop_supervised!(Connection)
      assert_receive {:DOWN, ^owner_ref, :process, _, _}, 1_000

      replacement =
        start_supervised!(%{
          id: :replacement,
          start: {Connection, :start_link, [[connection: options]]}
        })

      assert {:error, %Error{reason: :connecting}} = Connection.checkout_channel(replacement)
      assert Process.whereis(Session) == session
      send(server, :finish)
      assert Task.await(broker, 5_000) == :closed
      assert_receive {:connection_event, :stop, %{duration: duration}}
      assert duration >= 0
      assert_receive {:DOWN, ^session_ref, :process, _, :normal}, 1_000
    end
  end

  test "owner death before consuming the successful open result closes the connection" do
    {broker, port} = AMQPHandshake.start(self())
    owner = start_supervised!({Connection, connection: [host: "127.0.0.1", port: port]}, restart: :temporary)
    assert {:error, %Error{reason: :connecting}} = Connection.checkout_channel(owner)
    assert_receive {:handshake_waiting, server}, 2_000
    :ok = :sys.suspend(owner)
    session_ref = Process.monitor(Process.whereis(Session))

    test_pid = self()
    event = [:broadway_rabbitmq, :amqp, :open_connection, :stop]
    id = make_ref()
    :ok = :telemetry.attach(id, event, fn _, _, metadata, _ -> send(test_pid, {:opened, metadata.result}) end, nil)
    on_exit(fn -> :telemetry.detach(id) end)
    send(server, :finish)
    assert_receive {:opened, {:ok, conn}}, 2_000
    ref = Process.monitor(conn.pid)
    Process.exit(owner, :kill)
    assert Task.await(broker, 5_000) == :closed
    assert_receive {:DOWN, ^ref, :process, _, _}, 1_000
    assert_receive {:DOWN, ^session_ref, :process, _, _}, 1_000
  end

  test "unexpected messages crash the connection owner" do
    owner = start_supervised!({Connection, connection: []}, restart: :temporary)
    ref = Process.monitor(owner)
    send(owner, :unexpected)
    assert_receive {:DOWN, ^ref, :process, ^owner, {:function_clause, _}}
  end

  test "unexpected session messages close the socket and fail the owner" do
    {broker, port} = AMQPHandshake.start(self())
    owner = start_supervised!({Connection, connection: [host: "127.0.0.1", port: port]}, restart: :temporary)
    owner_ref = Process.monitor(owner)
    assert {:error, %Error{reason: :connecting}} = Connection.checkout_channel(owner)
    assert_receive {:handshake_waiting, server}, 2_000
    session = Process.whereis(Session)
    session_ref = Process.monitor(session)
    send(server, :finish)
    # get_state waits for the connect continuation to finish.
    assert %{connection: %{pid: connection}} = :sys.get_state(session)
    connection_ref = Process.monitor(connection)
    send(session, :unexpected)
    assert Task.await(broker, 5_000) == :closed
    assert_receive {:DOWN, ^connection_ref, :process, _, _}, 1_000
    assert_receive {:DOWN, ^session_ref, :process, _, {:function_clause, _}}, 1_000
    assert_receive {:DOWN, ^owner_ref, :process, _, {:function_clause, _}}, 1_000
  end
end
