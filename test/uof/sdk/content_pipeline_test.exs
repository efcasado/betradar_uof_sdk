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

  defmodule RaisingHandler do
    @moduledoc false
    use MessageHandler

    @impl true
    def handle_odds_change(_msg, _ctx), do: raise("handler failed")
  end

  setup do
    Application.put_env(:uof_sdk_test, :test_pid, self())
    :ok
  end

  defp start_pipeline(context, opts \\ []) do
    name = Module.concat(__MODULE__, context.test)

    defaults = [name: name, handler: Handler, concurrency: 1, producer: {Broadway.DummyProducer, []}]
    start_link_supervised!(%{id: name, start: {ContentPipeline, :start_link, [Keyword.merge(defaults, opts)]}})

    name
  end

  # Metadata as the Pulsar producer delivers it from the RabbitMQ source connector.
  defp send_message(pipeline, xml, routing_key, properties \\ %{}) do
    properties = Map.put(properties, "__rabbitmq_routing_key", routing_key)
    Broadway.test_message(pipeline, xml, metadata: %{properties: properties})
  end

  test "decodes and dispatches an odds_change with context", context do
    pipeline = start_pipeline(context)
    xml = ~s(<odds_change product="1" event_id="sr:match:12345" timestamp="42"/>)

    send_message(pipeline, xml, "hi.-.live.odds_change.1.sr:match.12345.-")

    assert_receive {:odds_change, %Feed.OddsChange{} = msg, ctx}, 1_000
    assert msg.event_id == "sr:match:12345"
    assert ctx.producer_id == 1
    assert ctx.routing_key == "hi.-.live.odds_change.1.sr:match.12345.-"
    assert ctx.message_type == "odds_change"
    assert ctx.event_urn == "sr:match:12345"
  end

  test "marks a message failed on undecodable data", context do
    ref = context |> start_pipeline() |> send_message("<nonsense/>", "x")
    assert_receive {:ack, ^ref, [], [_failed]}, 1_000
  end

  test "marks a message failed when its handler raises", context do
    pipeline = start_pipeline(context, handler: RaisingHandler)

    ref =
      send_message(
        pipeline,
        ~s(<odds_change product="3" event_id="sr:match:1" timestamp="99"/>),
        "hi.pre.-.odds_change.1.sr:match.1.-"
      )

    assert_receive {:ack, ^ref, [], [_failed]}, 1_000
  end

  test "acks alives without dispatching them", context do
    ref =
      context
      |> start_pipeline()
      |> send_message(~s(<alive product="1" timestamp="1" subscribed="1"/>), "-.-.-.alive.-.-.-.-")

    assert_receive {:ack, ^ref, [_successful], []}, 1_000
    refute_received {:odds_change, _, _}
  end

  test "acks another node's messages without dispatching them", context do
    pipeline = start_pipeline(context, node_id: 7)

    other =
      send_message(
        pipeline,
        ~s(<odds_change product="1" event_id="sr:match:1" timestamp="1"/>),
        "lo.pre.-.odds_change.1.sr:match.1.8",
        %{"__rabbitmq_consumer_tag" => "ctag-1"}
      )

    assert_receive {:ack, ^other, [_successful], []}, 1_000
    refute_received {:odds_change, _, _}

    send_message(
      pipeline,
      ~s(<odds_change product="1" event_id="sr:match:1" timestamp="2"/>),
      "lo.pre.-.odds_change.1.sr:match.1.7"
    )

    send_message(
      pipeline,
      ~s(<odds_change product="1" event_id="sr:match:1" timestamp="3"/>),
      "hi.-.live.odds_change.1.sr:match.1.-"
    )

    assert_receive {:odds_change, _, %{routing_key: "lo.pre.-.odds_change.1.sr:match.1.7"}}, 1_000
    assert_receive {:odds_change, _, %{routing_key: "hi.-.live.odds_change.1.sr:match.1.-"}}, 1_000
  end

  test "reports messages addressed to another node", context do
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

    context
    |> start_pipeline(node_id: 7)
    |> send_message(
      ~s(<odds_change product="1" event_id="sr:match:1" timestamp="1"/>),
      "lo.pre.-.odds_change.1.sr:match.1.8"
    )

    assert_receive {:other_node, %{count: 1}, %{node_id: "8", message_type: "odds_change"}}, 1_000
  end
end
