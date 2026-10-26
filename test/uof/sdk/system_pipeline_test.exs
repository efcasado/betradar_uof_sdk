defmodule UOF.SDK.SystemPipelineTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.SystemPipeline

  defmodule Sink do
    @moduledoc false
    def alive(product, ts, subscribed?, consumer_tag), do: emit({:alive_sink, product, ts, subscribed?, consumer_tag})
    def snapshot_complete(product, request_id), do: emit({:snapshot_sink, product, request_id})

    defp emit(event), do: send(Application.fetch_env!(:uof_sdk_test, :test_pid), event)
  end

  setup do
    Application.put_env(:uof_sdk_test, :test_pid, self())
    :ok
  end

  defp start_pipeline(context, opts \\ []) do
    name = Module.concat(__MODULE__, context.test)

    defaults = [name: name, producer: {Broadway.DummyProducer, []}, monitor: Sink]
    start_link_supervised!(%{id: name, start: {SystemPipeline, :start_link, [Keyword.merge(defaults, opts)]}})

    name
  end

  # Metadata as the Pulsar producer delivers it from the RabbitMQ source connector.
  defp send_message(pipeline, xml, routing_key, properties \\ %{}) do
    properties = Map.put(properties, "__rabbitmq_routing_key", routing_key)
    Broadway.test_message(pipeline, xml, metadata: %{properties: properties})
  end

  test "routes system side-effects to the monitor", context do
    pipeline = start_pipeline(context)

    send_message(pipeline, ~s(<alive product="1" timestamp="42" subscribed="1"/>), "-.-.-.alive.-.-.-.-")
    assert_receive {:alive_sink, 1, 42, true, nil}

    send_message(
      pipeline,
      ~s(<snapshot_complete product="3" timestamp="1" request_id="77"/>),
      "-.-.-.snapshot_complete.-.-.-.39"
    )

    assert_receive {:snapshot_sink, 3, 77}
  end

  test "routes an unsubscribed alive even when its timestamp is missing", context do
    context
    |> start_pipeline()
    |> send_message(~s(<alive product="1" subscribed="0"/>), "-.-.-.alive.-.-.-.-")

    assert_receive {:alive_sink, 1, nil, false, nil}
  end

  test "reports each alive with its upstream consumer tag", context do
    context
    |> start_pipeline()
    |> send_message(~s(<alive product="1" timestamp="42" subscribed="1"/>), "-.-.-.alive.-.-.-.-", %{
      "__rabbitmq_queue_name" => "uof-system",
      "__rabbitmq_consumer_tag" => "ctag-1"
    })

    assert_receive {:alive_sink, 1, 42, true, "ctag-1"}
  end

  test "acks without a monitor", context do
    ref =
      context
      |> start_pipeline(monitor: nil)
      |> send_message(~s(<alive product="1" timestamp="42" subscribed="1"/>), "-.-.-.alive.-.-.-.-")

    assert_receive {:ack, ^ref, [_successful], []}, 1_000
  end

  test "acks event content without decoding it", context do
    ref =
      context
      |> start_pipeline()
      |> send_message("<not valid UOF XML>", "hi.-.live.odds_change.1.sr:match.1.-", %{
        "__rabbitmq_consumer_tag" => "ctag-1"
      })

    assert_receive {:ack, ^ref, [_successful], []}, 1_000
    refute_received {:alive_sink, _, _, _, _}
    refute_received {:snapshot_sink, _, _}
  end

  test "acks another node's snapshot_complete without reaching the monitor", context do
    pipeline = start_pipeline(context, node_id: 7)

    other =
      send_message(
        pipeline,
        ~s(<snapshot_complete product="3" timestamp="1" request_id="1"/>),
        "-.-.-.snapshot_complete.-.-.-.8"
      )

    assert_receive {:ack, ^other, [_successful], []}, 1_000
    refute_received {:snapshot_sink, _, _}

    send_message(
      pipeline,
      ~s(<snapshot_complete product="3" timestamp="1" request_id="1"/>),
      "-.-.-.snapshot_complete.-.-.-.7"
    )

    assert_receive {:snapshot_sink, 3, 1}
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
      ~s(<snapshot_complete product="3" timestamp="1" request_id="1"/>),
      "-.-.-.snapshot_complete.-.-.-.8"
    )

    assert_receive {:other_node, %{count: 1}, %{node_id: "8", message_type: "snapshot_complete"}}, 1_000
  end
end
