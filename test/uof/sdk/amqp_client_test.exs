defmodule UOF.SDK.AMQP.ClientTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.AMQP.Client

  defmodule UnavailablePool do
    @moduledoc false
    @behaviour BroadwayRabbitMQ.ChannelPool

    @impl true
    def checkout_channel(test_pid) do
      if is_pid(test_pid), do: send(test_pid, {:checkout, self()})
      {:error, RuntimeError.exception("broker unavailable")}
    end

    @impl true
    def checkin_channel(_, _), do: :ok
  end

  test "pool failures use Broadway's reconnect backoff instead of crashing its producer" do
    assert {:ok, config} =
             Client.init(
               queue: "",
               declare: [exclusive: true],
               connection: {:custom_pool, UnavailablePool, []}
             )

    assert {:error, :econnrefused} = Client.setup_channel(config)
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
end
