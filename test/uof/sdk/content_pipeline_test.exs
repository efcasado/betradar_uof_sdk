defmodule UOF.SDK.ContentPipelineTest do
  use ExUnit.Case, async: false

  alias UOF.Schemas.Feed
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.MessageHandler

  # Test handler: forwards every callback to the pid registered in app env.
  defmodule Handler do
    @moduledoc false
    use MessageHandler

    @impl true
    def handle_odds_change(msg, ctx), do: notify({:odds_change, msg, ctx})

    defp notify(event), do: send(Application.fetch_env!(:uof_sdk_test, :test_pid), event)
  end

  # One module standing in for monitor/recovery lifecycle notifications.
  defmodule Sink do
    @moduledoc false

    def snapshot_complete(product, request_id), do: emit({:snapshot_sink, product, request_id})
    def observe_connection(tag), do: emit({:connection_sink, tag})

    defp emit(event), do: send(Application.fetch_env!(:uof_sdk_test, :test_pid), event)
  end

  defmodule RaisingHandler do
    @moduledoc false
    use MessageHandler

    @impl true
    def handle_odds_change(_msg, _ctx), do: raise("handler failed")
  end

  setup context do
    Application.put_env(:uof_sdk_test, :test_pid, self())
    name = Module.concat(__MODULE__, context.test)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []}
           ]
         ]}
    })

    %{pipeline: name}
  end

  test "decodes and dispatches an odds_change with context", %{pipeline: pipeline} do
    xml = ~s(<odds_change product="1" event_id="sr:match:12345" timestamp="42"/>)
    metadata = %{routing_key: "hi.-.live.odds_change.1.sr:match.12345.-"}

    Broadway.test_message(pipeline, xml, metadata: metadata)

    assert_receive {:odds_change, %Feed.OddsChange{} = msg, ctx}, 1_000
    assert msg.event_id == "sr:match:12345"
    assert ctx.producer_id == 1
    assert ctx.message_type == "odds_change"
    assert ctx.event_urn == "sr:match:12345"
  end

  test "marks a message failed on undecodable data", %{pipeline: pipeline} do
    ref = Broadway.test_message(pipeline, "<nonsense/>", metadata: %{routing_key: "x"})
    assert_receive {:ack, ^ref, [], [_failed]}, 1_000
  end

  test "delivers content and alives without reporting them to the monitor" do
    name = Module.concat(__MODULE__, Sinks)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink
           ]
         ]}
    })

    # The sink has no function for content or alive reports, so making one would fail the message.
    content =
      Broadway.test_message(name, ~s(<odds_change product="3" event_id="sr:match:1" timestamp="99"/>),
        metadata: %{routing_key: "hi.pre.-.odds_change.1.sr:match.1.-"}
      )

    alive =
      Broadway.test_message(name, ~s(<alive product="3" timestamp="123" subscribed="1"/>),
        metadata: %{routing_key: "-.-.-.alive.-.-.-.-"}
      )

    assert_receive {:ack, ^content, [_successful], []}, 1_000
    assert_receive {:ack, ^alive, [_successful], []}, 1_000
    assert_receive {:odds_change, _, _}
  end

  test "fails a message whose handler raises" do
    name = Module.concat(__MODULE__, HandlerFailure)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: RaisingHandler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink
           ]
         ]}
    })

    ref =
      Broadway.test_message(name, ~s(<odds_change product="3" event_id="sr:match:1" timestamp="99"/>),
        metadata: %{routing_key: "hi.pre.-.odds_change.1.sr:match.1.-"}
      )

    assert_receive {:ack, ^ref, [], [_failed]}, 1_000
  end

  test "reads the routing key from a custom AMQP metadata field" do
    name = Module.concat(__MODULE__, CustomRk)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             routing_key_metadata_key: :custom_key
           ]
         ]}
    })

    xml = ~s(<odds_change product="1" event_id="sr:match:12345" timestamp="42"/>)
    # routing key lives under :custom_key, not :routing_key
    metadata = %{custom_key: "hi.-.live.odds_change.1.sr:match.12345.-"}

    Broadway.test_message(name, xml, metadata: metadata)

    assert_receive {:odds_change, %Feed.OddsChange{}, ctx}, 1_000
    assert ctx.message_type == "odds_change"
    assert ctx.event_urn == "sr:match:12345"
  end

  test "reads the routing key from the Pulsar RabbitMQ source property" do
    name = Module.concat(__MODULE__, PulsarRoutingKeyProperty)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             metadata_adapter: :pulsar_rabbitmq_source
           ]
         ]}
    })

    xml = ~s(<odds_change product="1" event_id="sr:match:12345" timestamp="42"/>)

    metadata = %{
      properties: %{"__rabbitmq_routing_key" => "hi.-.live.odds_change.1.sr:match.12345.-"}
    }

    Broadway.test_message(name, xml, metadata: metadata)

    assert_receive {:odds_change, %Feed.OddsChange{}, ctx}, 1_000
    assert ctx.routing_key == "hi.-.live.odds_change.1.sr:match.12345.-"
    assert ctx.message_type == "odds_change"
    assert ctx.event_urn == "sr:match:12345"
  end

  test "observes the content AMQP consumer tag from content-session alives" do
    name = Module.concat(__MODULE__, ConnObserve)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink
           ]
         ]}
    })

    Broadway.test_message(name, ~s(<alive product="1" timestamp="1" subscribed="1"/>),
      metadata: %{routing_key: "-.-.-.alive.-.-.-.-", consumer_tag: "amq.ctag-content-1"}
    )

    assert_receive {:connection_sink, {:content, "amq.ctag-content-1"}}
  end

  test "does not observe the content AMQP connection on event messages" do
    name = Module.concat(__MODULE__, EventConnIgnored)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink
           ]
         ]}
    })

    Broadway.test_message(name, ~s(<odds_change product="1" event_id="sr:match:1" timestamp="1"/>),
      metadata: %{routing_key: "hi.pre.-.odds_change.1.sr:match.1.-", consumer_tag: "amq.ctag-content-1"}
    )

    assert_receive {:odds_change, _, _}
    refute_received {:connection_sink, _}
  end

  test "observes a consumer tag from Pulsar RabbitMQ source properties" do
    name = Module.concat(__MODULE__, PulsarConsumerTag)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             metadata_adapter: :pulsar_rabbitmq_source,
             monitor: Sink
           ]
         ]}
    })

    Broadway.test_message(name, ~s(<alive product="1" timestamp="1" subscribed="1"/>),
      metadata: %{
        properties: %{
          "__rabbitmq_routing_key" => "-.-.-.alive.-.-.-.-",
          "__rabbitmq_queue_name" => "uof-content",
          "__rabbitmq_consumer_tag" => "ctag-1"
        }
      }
    )

    assert_receive {:connection_sink, {:content, "ctag-1"}}
  end

  test "observes a Pulsar RabbitMQ source consumer tag from event messages" do
    name = Module.concat(__MODULE__, PulsarEventConsumerTag)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             metadata_adapter: :pulsar_rabbitmq_source,
             monitor: Sink
           ]
         ]}
    })

    Broadway.test_message(name, ~s(<odds_change product="1" event_id="sr:match:1" timestamp="1"/>),
      metadata: %{
        properties: %{
          "__rabbitmq_routing_key" => "hi.-.live.odds_change.1.sr:match.1.-",
          "__rabbitmq_queue_name" => "uof-content",
          "__rabbitmq_consumer_tag" => "ctag-1"
        }
      }
    )

    assert_receive {:odds_change, _, _}
    assert_receive {:connection_sink, {:content, "ctag-1"}}
  end

  test "acks another node's messages without dispatching them" do
    name = Module.concat(__MODULE__, OtherNode)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             metadata_adapter: :pulsar_rabbitmq_source,
             monitor: Sink,
             node_id: 7
           ]
         ]}
    })

    other =
      Broadway.test_message(name, ~s(<odds_change product="1" event_id="sr:match:1" timestamp="1"/>),
        metadata: %{
          properties: %{
            "__rabbitmq_routing_key" => "lo.pre.-.odds_change.1.sr:match.1.8",
            "__rabbitmq_consumer_tag" => "ctag-1"
          }
        }
      )

    assert_receive {:ack, ^other, [_successful], []}, 1_000
    refute_received {:odds_change, _, _}
    refute_received {:connection_sink, _}

    Broadway.test_message(name, ~s(<odds_change product="1" event_id="sr:match:1" timestamp="2"/>),
      metadata: %{
        properties: %{"__rabbitmq_routing_key" => "lo.pre.-.odds_change.1.sr:match.1.7"}
      }
    )

    Broadway.test_message(name, ~s(<odds_change product="1" event_id="sr:match:1" timestamp="3"/>),
      metadata: %{
        properties: %{"__rabbitmq_routing_key" => "hi.-.live.odds_change.1.sr:match.1.-"}
      }
    )

    assert_receive {:odds_change, _, %{routing_key: "lo.pre.-.odds_change.1.sr:match.1.7"}}, 1_000
    assert_receive {:odds_change, _, %{routing_key: "hi.-.live.odds_change.1.sr:match.1.-"}}, 1_000
  end

  test "reports messages addressed to another node" do
    name = Module.concat(__MODULE__, OtherNodeTelemetry)
    test_pid = self()
    id = make_ref()

    :telemetry.attach(
      id,
      [:uof_sdk, :message, :other_node],
      fn _event, measurements, metadata, _config ->
        send(test_pid, {:other_node, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    start_link_supervised!(%{
      id: name,
      start:
        {ContentPipeline, :start_link,
         [[name: name, handler: Handler, concurrency: 1, producer: {Broadway.DummyProducer, []}, node_id: 7]]}
    })

    Broadway.test_message(name, ~s(<odds_change product="1" event_id="sr:match:1" timestamp="1"/>),
      metadata: %{routing_key: "lo.pre.-.odds_change.1.sr:match.1.8"}
    )

    assert_receive {:other_node, %{count: 1}, %{node_id: "8", message_type: "odds_change"}}, 1_000
  end
end
