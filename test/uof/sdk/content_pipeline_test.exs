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

    defp notify(event), do: send(Application.fetch_env!(:uof_sdk, :test_pid), event)
  end

  # One module standing in for monitor/recovery lifecycle notifications.
  defmodule Sink do
    @moduledoc false
    def message(product, ts), do: emit({:message_sink, product, ts})
    def snapshot_complete(product, request_id), do: emit({:snapshot_sink, product, request_id})
    def observe_connection(token), do: emit({:connection_sink, token})

    defp emit(event), do: send(Application.fetch_env!(:uof_sdk, :test_pid), event)
  end

  defmodule RaisingHandler do
    @moduledoc false
    use MessageHandler

    @impl true
    def handle_odds_change(_msg, _ctx), do: raise("handler failed")
  end

  setup context do
    Application.put_env(:uof_sdk, :test_pid, self())
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

  test "routes content timestamps to the monitor" do
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

    Broadway.test_message(name, ~s(<odds_change product="3" event_id="sr:match:1" timestamp="99"/>),
      metadata: %{routing_key: "hi.pre.-.odds_change.1.sr:match.1.-"}
    )

    assert_receive {:message_sink, 3, 99}
  end

  test "routes content-session alive timestamps to the monitor" do
    name = Module.concat(__MODULE__, ContentAlive)

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

    Broadway.test_message(name, ~s(<alive product="3" timestamp="123" subscribed="1"/>),
      metadata: %{routing_key: "-.-.-.alive.-.-.-.-"}
    )

    assert_receive {:message_sink, 3, 123}
    refute_received {:odds_change, _, _}
  end

  test "does not notify lifecycle side-effects when handler delivery fails" do
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
    refute_received {:message_sink, 3, 99}
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

  test "reads the routing key from the Pulsar RabbitMQ source partition key" do
    name = Module.concat(__MODULE__, PulsarRabbitMQSource)

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
    metadata = %{metadata: %{partition_key: "hi.-.live.odds_change.1.sr:match.12345.-"}}

    Broadway.test_message(name, xml, metadata: metadata)

    assert_receive {:odds_change, %Feed.OddsChange{}, ctx}, 1_000
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

  test "observes a content connection token from a custom metadata field" do
    name = Module.concat(__MODULE__, CustomToken)

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
             monitor: Sink,
             connection_token_metadata_key: :conn_id
           ]
         ]}
    })

    Broadway.test_message(name, ~s(<alive product="1" timestamp="1" subscribed="1"/>),
      metadata: %{routing_key: "-.-.-.alive.-.-.-.-", conn_id: "conn-abc-123"}
    )

    assert_receive {:connection_sink, {:content, "conn-abc-123"}}
  end

  test "observes a connection token from Pulsar RabbitMQ source properties" do
    name = Module.concat(__MODULE__, PulsarConnToken)

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
        metadata: %{partition_key: "-.-.-.alive.-.-.-.-"},
        single_metadata: %{
          properties: [
            %{key: "__rabbitmq_queue_name", value: "uof-content"},
            %{key: "__rabbitmq_consumer_tag", value: "ctag-1"}
          ]
        }
      }
    )

    assert_receive {:message_sink, 1, 1}
    assert_receive {:connection_sink, {:content, "ctag-1"}}
  end

  test "observes a Pulsar RabbitMQ source connection token from event messages" do
    name = Module.concat(__MODULE__, PulsarEventConnToken)

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
        metadata: %{
          partition_key: "hi.-.live.odds_change.1.sr:match.1.-",
          properties: [
            %{key: "__rabbitmq_queue_name", value: "uof-content"},
            %{key: "__rabbitmq_consumer_tag", value: "ctag-1"}
          ]
        }
      }
    )

    assert_receive {:odds_change, _, _}
    assert_receive {:connection_sink, {:content, "ctag-1"}}
  end
end
