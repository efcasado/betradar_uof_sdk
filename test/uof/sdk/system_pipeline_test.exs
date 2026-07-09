defmodule UOF.SDK.SystemPipelineTest do
  use ExUnit.Case, async: false

  alias UOF.Schemas.Feed
  alias UOF.SDK.MessageHandler
  alias UOF.SDK.SystemPipeline

  defmodule Handler do
    @moduledoc false
    use MessageHandler

    @impl true
    def handle_alive(msg, ctx), do: notify({:alive, msg, ctx})

    defp notify(event), do: send(Application.fetch_env!(:uof_sdk, :test_pid), event)
  end

  defmodule Sink do
    @moduledoc false
    def alive(product, ts, subscribed?), do: emit({:alive_sink, product, ts, subscribed?})
    def snapshot_complete(product, request_id), do: emit({:snapshot_sink, product, request_id})
    def observe_connection(conn_pid), do: emit({:connection_sink, conn_pid})

    defp emit(event), do: send(Application.fetch_env!(:uof_sdk, :test_pid), event)
  end

  defmodule RaisingHandler do
    @moduledoc false
    use MessageHandler

    @impl true
    def handle_alive(_msg, _ctx), do: raise("handler failed")
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
             handler: Handler,
             producer: {Broadway.DummyProducer, []}
           ]
         ]}
    })

    %{pipeline: name}
  end

  test "dispatches an alive heartbeat", %{pipeline: pipeline} do
    xml = ~s(<alive product="1" timestamp="42" subscribed="1"/>)
    metadata = %{routing_key: "-.-.-.alive.-.-.-.-"}

    Broadway.test_message(pipeline, xml, metadata: metadata)

    assert_receive {:alive, %Feed.Alive{product: 1}, ctx}, 1_000
    assert ctx.message_type == "alive"
    assert ctx.event_urn == nil
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
             handler: Handler,
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

  test "does not notify lifecycle side-effects when handler delivery fails" do
    name = Module.concat(__MODULE__, HandlerFailure)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             handler: RaisingHandler,
             producer: {Broadway.DummyProducer, []},
             monitor: Sink
           ]
         ]}
    })

    ref =
      Broadway.test_message(name, ~s(<alive product="1" timestamp="99" subscribed="1"/>),
        metadata: %{routing_key: "-.-.-.alive.-.-.-.-"}
      )

    assert_receive {:ack, ^ref, [], [_failed]}, 1_000
    refute_received {:alive_sink, 1, 99, true}
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
             handler: Handler,
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

  test "reads the routing key from a custom metadata field" do
    name = Module.concat(__MODULE__, CustomRk)

    start_link_supervised!(%{
      id: name,
      start:
        {SystemPipeline, :start_link,
         [
           [
             name: name,
             handler: Handler,
             producer: {Broadway.DummyProducer, []},
             routing_key_metadata_key: :pulsar_key
           ]
         ]}
    })

    xml = ~s(<alive product="1" timestamp="42" subscribed="1"/>)
    metadata = %{pulsar_key: "-.-.-.alive.-.-.-.-"}

    Broadway.test_message(name, xml, metadata: metadata)

    assert_receive {:alive, %Feed.Alive{}, ctx}, 1_000
    assert ctx.message_type == "alive"
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
             handler: Handler,
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
