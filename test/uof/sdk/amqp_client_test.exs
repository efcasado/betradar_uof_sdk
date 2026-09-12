defmodule UOF.SDK.AMQP.ClientTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.AMQP.Client
  alias UOF.SDK.AMQP.Error

  defmodule UnavailablePool do
    @moduledoc false
    @behaviour BroadwayRabbitMQ.ChannelPool

    @impl true
    def checkout_channel(test_pid) do
      if is_pid(test_pid), do: send(test_pid, {:checkout, self()})
      {:error, %Error{operation: :connect, reason: :econnrefused}}
    end

    @impl true
    def checkin_channel(_, _), do: :ok
  end

  defmodule ExitingPool do
    @moduledoc false
    @behaviour BroadwayRabbitMQ.ChannelPool

    @impl true
    def checkout_channel(reason), do: exit(reason)

    @impl true
    def checkin_channel(_, _), do: :ok
  end

  defmodule RejectedPool do
    @moduledoc false
    @behaviour BroadwayRabbitMQ.ChannelPool

    @impl true
    def checkout_channel(reason), do: {:error, %Error{operation: :connect, reason: reason}}
    @impl true
    def checkin_channel(_, _), do: :ok
  end

  defp config(pool, args) do
    assert {:ok, config} = Client.init(queue: "", declare: [exclusive: true], connection: {:custom_pool, pool, args})
    config
  end

  test "a pool checkout exception is translated to a reason Broadway backs off from" do
    assert {:error, :econnrefused} = Client.setup_channel(config(UnavailablePool, []))
  end

  test "unexpected pool exits propagate unchanged" do
    assert catch_exit(Client.setup_channel(config(ExitingPool, :pool_bug))) == :pool_bug
  end

  test "bare process exits are not classified as transport failures" do
    refute Error.retryable?(:normal)
    refute Error.retryable?(:shutdown)
    refute Error.retryable?({:shutdown, :pool_bug})
    refute Error.retryable?({:timeout, :unrelated_operation})
    assert Error.retryable?({:noproc, {GenServer, :call, [self(), :connection, 5_000]}})
  end

  test "an unavailable connection is retried by the same Broadway producer" do
    start_supervised!(
      {UOF.SDK.ContentPipeline,
       name: __MODULE__.Pipeline,
       handler: UOF.SDK.LogHandler,
       concurrency: 1,
       producer:
         {BroadwayRabbitMQ.Producer,
          queue: "",
          declare: [exclusive: true],
          client: Client,
          on_failure: :reject,
          connection: {:custom_pool, UnavailablePool, self()},
          backoff_min: 10,
          backoff_max: 20}}
    )

    assert_receive {:checkout, producer}, 1_000
    assert_receive {:checkout, ^producer}, 1_000
    assert Process.alive?(producer)
  end

  test "authentication failures retain their reason and do not use the network backoff alias" do
    reason = {:auth_failure, ~c"ACCESS_REFUSED"}
    test_pid = self()
    id = make_ref()
    :telemetry.attach(id, [:uof_sdk, :amqp, :setup_failure], fn _, _, metadata, _ -> send(test_pid, metadata) end, nil)
    on_exit(fn -> :telemetry.detach(id) end)
    assert {:error, ^reason} = Client.setup_channel(config(RejectedPool, reason))
    assert_receive %{operation: :connect, reason: ^reason, retryable: false}
  end

  test "only connection-level broker closes are retryable" do
    assert Error.retryable?({:shutdown, {:server_initiated_close, 320, "forced"}})
    refute Error.retryable?({:shutdown, {:server_initiated_close, 404, "missing exchange"}})
    refute Error.retryable?({:server_initiated_close, 530, "invalid vhost"})
  end
end
