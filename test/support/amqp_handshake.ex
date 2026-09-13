defmodule UOF.SDK.TestSupport.AMQPHandshake do
  @moduledoc false
  import ExUnit.Assertions

  # A minimal broker handshake lets tests pause Connection.open before it
  # returns, without timing a real broker or sleeping through a network timeout.
  def start(test_pid) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    task =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)

        try do
          assert {:ok, <<"AMQP", 0, 0, 9, 1>>} = :gen_tcp.recv(socket, 8, 2_000)
          send(test_pid, {:handshake_waiting, self()})

          receive do
            :finish -> :ok
          after
            5_000 -> flunk("test did not release handshake")
          end

          send_method(socket, {:"connection.start", 0, 9, [], "PLAIN", "en_US"})
          assert {10, 11} = receive_method(socket)
          send_method(socket, {:"connection.tune", 0, 131_072, 0})
          assert {10, 31} = receive_method(socket)
          assert {10, 40} = receive_method(socket)
          send_method(socket, {:"connection.open_ok", ""})
          assert {10, 50} = receive_method(socket)
          send_method(socket, {:"connection.close_ok"})
          assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
          :closed
        after
          :gen_tcp.close(socket)
          :gen_tcp.close(listener)
        end
      end)

    {task, port}
  end

  defp send_method(socket, method) do
    :ok = :gen_tcp.send(socket, :rabbit_binary_generator.build_simple_method_frame(0, method, :rabbit_framing_amqp_0_9_1))
  end

  defp receive_method(socket) do
    {:ok, <<1, 0::16, size::32>>} = :gen_tcp.recv(socket, 7, 2_000)
    {:ok, <<class::16, method::16, _rest::binary>>} = :gen_tcp.recv(socket, size + 1, 2_000)
    {class, method}
  end
end
