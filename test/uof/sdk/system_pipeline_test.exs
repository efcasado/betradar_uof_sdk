defmodule UOF.SDK.SystemPipelineTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.SystemPipeline

  defmodule Sink do
    @moduledoc false
    def alive(product, ts, subscribed?), do: emit({:alive_sink, product, ts, subscribed?})
    def snapshot_complete(product, request_id), do: emit({:snapshot_sink, product, request_id})
    def observe_connection(conn_pid), do: emit({:connection_sink, conn_pid})

    defp emit(event), do: send(Application.fetch_env!(:uof_sdk, :test_pid), event)
  end

  setup context do
    Application.put_env(:uof_sdk, :test_pid, self())
    name = Module.concat(__MODULE__, context.test)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             producer: {Broadway.DummyProducer, []}
           ]
         ]}
    })

    %{pipeline: name}
  end

  test "routes system side-effects to the monitor" do
    name = Module.concat(__MODULE__, Sinks)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink
           ]
         ]}
    })

    Broadway.test_message(name, ~s(<alive product="1" timestamp="42" subscribed="1"/>),
      metadata: %{routing_key: "-.-.-.alive.-.-.-.-"}
    )

    assert_receive {:alive_sink, 1, 42, true}

    Broadway.test_message(name, ~s(<snapshot_complete product="3" timestamp="1" request_id="77"/>),
      metadata: %{routing_key: "-.-.-.snapshot_complete.-.-.-.39"}
    )

    assert_receive {:snapshot_sink, 3, 77}
  end

  test "observes the AMQP connection pid for reconnect detection" do
    name = Module.concat(__MODULE__, ConnObserve)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink
           ]
         ]}
    })

    conn = spawn(fn -> :ok end)

    Broadway.test_message(name, ~s(<alive product="1" timestamp="1" subscribed="1"/>),
      metadata: %{routing_key: "-.-.-.alive.-.-.-.-", amqp_channel: %{conn: %{pid: conn}}}
    )

    assert_receive {:connection_sink, {:system, ^conn}}
  end

  test "reads the routing key from a custom AMQP metadata field" do
    name = Module.concat(__MODULE__, CustomRk)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             producer: {Broadway.DummyProducer, []},
             routing_key_metadata_key: :custom_key
           ]
         ]}
    })

    xml = ~s(<alive product="1" timestamp="42" subscribed="1"/>)
    metadata = %{custom_key: "-.-.-.alive.-.-.-.-"}

    ref = Broadway.test_message(name, xml, metadata: metadata)

    assert_receive {:ack, ^ref, [_successful], []}, 1_000
  end

  test "reads the routing key from the Pulsar RabbitMQ source partition key" do
    name = Module.concat(__MODULE__, PulsarRabbitMQSource)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             producer: {Broadway.DummyProducer, []},
             metadata_adapter: :pulsar_rabbitmq_source,
             monitor: Sink
           ]
         ]}
    })

    Broadway.test_message(name, ~s(<alive product="1" timestamp="42" subscribed="1"/>),
      metadata: %{
        metadata: %{
          partition_key: "-.-.-.alive.-.-.-.-",
          properties: [
            %{key: "__rabbitmq_queue_name", value: "uof-system"},
            %{key: "__rabbitmq_consumer_tag", value: "ctag-1"}
          ]
        }
      }
    )

    assert_receive {:alive_sink, 1, 42, true}
    assert_receive {:connection_sink, {:system, {"uof-system", "ctag-1"}}}
  end

  test "acks non-system messages without decoding them" do
    name = Module.concat(__MODULE__, FastDiscard)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink
           ]
         ]}
    })

    ref =
      Broadway.test_message(name, "<not valid UOF XML>", metadata: %{routing_key: "hi.-.live.odds_change.1.sr:match.1.-"})

    assert_receive {:ack, ^ref, [_successful], []}, 1_000
    refute_received {:alive_sink, _, _, _}
    refute_received {:snapshot_sink, _, _}
    refute_received {:connection_sink, _}
  end

  test "observes a connection token from a custom metadata field" do
    name = Module.concat(__MODULE__, CustomToken)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink,
             connection_token_metadata_key: :conn_id
           ]
         ]}
    })

    Broadway.test_message(name, ~s(<alive product="1" timestamp="1" subscribed="1"/>),
      metadata: %{routing_key: "-.-.-.alive.-.-.-.-", conn_id: "conn-abc-123"}
    )

    assert_receive {:connection_sink, {:system, "conn-abc-123"}}
  end
end
