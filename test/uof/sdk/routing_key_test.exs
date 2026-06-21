defmodule UOF.SDK.RoutingKeyTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.RoutingKey

  test "parses a live odds_change event key" do
    rk = RoutingKey.parse("hi.-.live.odds_change.1.sr:match.12345.-")

    assert rk.priority == "hi"
    assert rk.scope == "live"
    assert rk.message_type == "odds_change"
    assert rk.sport_id == 1
    assert rk.urn_type == "sr:match"
    assert rk.event_id == "12345"
    assert rk.event_urn == "sr:match:12345"
    assert rk.node_id == nil
    assert RoutingKey.partition_key(rk) == "sr:match:12345"
  end

  test "treats a system alive key as having no event" do
    rk = RoutingKey.parse("-.-.-.alive.-.-.-.-")

    assert rk.message_type == "alive"
    assert rk.sport_id == nil
    assert rk.event_urn == nil
    assert RoutingKey.partition_key(rk) == :system
  end

  test "parses snapshot_complete with a node id" do
    rk = RoutingKey.parse("-.-.-.snapshot_complete.-.-.-.3")

    assert rk.message_type == "snapshot_complete"
    assert rk.node_id == "3"
    assert RoutingKey.partition_key(rk) == :system
  end
end
