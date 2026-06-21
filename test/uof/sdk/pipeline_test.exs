defmodule UOF.SDK.PipelineTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.Feed

  # Test handler: forwards every callback to the pid registered in app env.
  defmodule Handler do
    use UOF.SDK.MessageHandler

    @impl true
    def handle_odds_change(msg, ctx), do: notify({:odds_change, msg, ctx})
    @impl true
    def handle_alive(msg, ctx), do: notify({:alive, msg, ctx})

    defp notify(event), do: send(Application.fetch_env!(:betradar_uof_sdk, :test_pid), event)
  end

  # One module standing in for monitor + recovery + checkpoint store; the
  # function names don't collide, so the pipeline can target it for all three.
  defmodule Sink do
    def alive(product, ts, subscribed?), do: emit({:alive_sink, product, ts, subscribed?})
    def message(product, ts), do: emit({:message_sink, product, ts})
    def product_down(product), do: emit({:product_down_sink, product})
    def snapshot_complete(product, request_id), do: emit({:snapshot_sink, product, request_id})
    def put(product, ts), do: emit({:checkpoint_sink, product, ts})
    def observe_connection(conn_pid), do: emit({:connection_sink, conn_pid})

    defp emit(event), do: send(Application.fetch_env!(:betradar_uof_sdk, :test_pid), event)
  end

  setup context do
    Application.put_env(:betradar_uof_sdk, :test_pid, self())
    name = Module.concat(__MODULE__, context.test)

    start_link_supervised!(%{
      id: name,
      start:
        {UOF.SDK.Pipeline, :start_link,
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

  test "dispatches an alive heartbeat", %{pipeline: pipeline} do
    xml = ~s(<alive product="1" timestamp="42" subscribed="1"/>)
    metadata = %{routing_key: "-.-.-.alive.-.-.-.-"}

    Broadway.test_message(pipeline, xml, metadata: metadata)

    assert_receive {:alive, %Feed.Alive{product: 1}, ctx}, 1_000
    assert ctx.message_type == "alive"
    assert ctx.event_urn == nil
  end

  test "marks a message failed on undecodable data", %{pipeline: pipeline} do
    ref = Broadway.test_message(pipeline, "<nonsense/>", metadata: %{routing_key: "x"})
    assert_receive {:ack, ^ref, [], [_failed]}, 1_000
  end

  test "routes lifecycle side-effects to monitor/recovery/checkpoint" do
    name = Module.concat(__MODULE__, Sinks)

    start_link_supervised!(%{
      id: name,
      start:
        {UOF.SDK.Pipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             concurrency: 1,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink,
             recovery: Sink,
             checkpoint_store: Sink
           ]
         ]}
    })

    # alive -> monitor.alive + checkpoint
    Broadway.test_message(name, ~s(<alive product="1" timestamp="42" subscribed="1"/>),
      metadata: %{routing_key: "-.-.-.alive.-.-.-.-"}
    )

    assert_receive {:alive_sink, 1, 42, true}
    assert_receive {:checkpoint_sink, 1, 42}

    # content message -> monitor.message + checkpoint
    Broadway.test_message(name, ~s(<odds_change product="3" event_id="sr:match:1" timestamp="99"/>),
      metadata: %{routing_key: "hi.pre.-.odds_change.1.sr:match.1.-"}
    )

    assert_receive {:message_sink, 3, 99}
    assert_receive {:checkpoint_sink, 3, 99}

    # snapshot_complete -> recovery
    Broadway.test_message(name, ~s(<snapshot_complete product="3" timestamp="1" request_id="77"/>),
      metadata: %{routing_key: "-.-.-.snapshot_complete.-.-.-.39"}
    )

    assert_receive {:snapshot_sink, 3, 77}

    # product_down -> monitor
    Broadway.test_message(name, ~s(<product_down product="3" timestamp="1"/>),
      metadata: %{routing_key: "-.-.-.product_down.-.-.-.-"}
    )

    assert_receive {:product_down_sink, 3}
  end

  test "observes the AMQP connection pid for reconnect detection" do
    name = Module.concat(__MODULE__, ConnObserve)

    start_link_supervised!(%{
      id: name,
      start:
        {UOF.SDK.Pipeline, :start_link,
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

    conn = spawn(fn -> :ok end)

    Broadway.test_message(name, ~s(<alive product="1" timestamp="1" subscribed="1"/>),
      metadata: %{routing_key: "-.-.-.alive.-.-.-.-", amqp_channel: %{conn: %{pid: conn}}}
    )

    assert_receive {:connection_sink, ^conn}
  end
end
