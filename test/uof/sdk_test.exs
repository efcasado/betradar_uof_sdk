defmodule UOF.SDKTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Config

  test "builds a single catch-all pipeline with the broadcast system keys" do
    config = Config.load(handler: MyApp.Handler, access_token: "tok", host: "stgmq.betradar.com")

    assert [{UOF.SDK.Pipeline, opts}] = UOF.SDK.child_specs(config)
    assert opts[:name] == UOF.SDK.Pipeline
    assert opts[:handler] == MyApp.Handler

    keys = for {"unifiedfeed", routing_key: rk} <- opts[:bindings], do: rk
    # 8th field (node) + trailing `#` for the optional producer id
    assert "*.*.*.*.*.*.*.-.#" in keys
    assert "*.*.*.*.*.*.*.#" in keys
    assert "-.-.-.alive.#" in keys
    assert "-.-.-.snapshot_complete.#" in keys
  end

  test "scopes the bindings to a configured node id" do
    config = Config.load(handler: MyApp.Handler, access_token: "tok", host: "stgmq.betradar.com", node_id: 7)
    [{UOF.SDK.Pipeline, opts}] = UOF.SDK.child_specs(config)

    keys = for {"unifiedfeed", routing_key: rk} <- opts[:bindings], do: rk
    # broadcast (`-`) and our node (`7`) only — not any node (`#`)
    assert "*.*.*.*.*.*.*.-.#" in keys
    assert "*.*.*.*.*.*.*.7.#" in keys
    refute "*.*.*.*.*.*.*.#" in keys
    assert "-.-.-.snapshot_complete.-.-.-.7" in keys
    refute "-.-.-.snapshot_complete.#" in keys
    # alive stays broadcast regardless of node id
    assert "-.-.-.alive.#" in keys
  end

  test "threads the resolved AMQP connection into the pipeline" do
    config = Config.load(handler: MyApp.Handler, access_token: "tok", host: "stgmq.betradar.com")
    [{UOF.SDK.Pipeline, opts}] = UOF.SDK.child_specs(config)

    assert opts[:connection][:username] == "tok"
    assert opts[:connection][:host] == "stgmq.betradar.com"
  end
end
