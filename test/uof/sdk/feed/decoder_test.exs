defmodule UOF.SDK.Feed.DecoderTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Feed
  alias UOF.SDK.Feed.Decoder

  test "decodes an alive heartbeat" do
    xml = ~s(<alive product="1" timestamp="1234567890" subscribed="1"/>)

    assert {:ok, %Feed.Alive{product: 1, timestamp: 1_234_567_890, subscribed: 1}} =
             Decoder.decode(xml)
  end

  test "decodes a snapshot_complete" do
    xml = ~s(<snapshot_complete product="3" timestamp="42" request_id="777"/>)

    assert {:ok, %Feed.SnapshotComplete{product: 3, request_id: 777, timestamp: 42}} =
             Decoder.decode(xml)
  end

  test "decodes an odds_change with nested odds/market/outcome" do
    xml = """
    <odds_change product="1" event_id="sr:match:12345" timestamp="1234567890">
      <odds>
        <market id="1" status="1">
          <outcome id="1" odds="1.85" active="1"/>
          <outcome id="2" odds="2.10" active="1"/>
        </market>
      </odds>
    </odds_change>
    """

    assert {:ok, odds_change} = Decoder.decode(xml)
    assert odds_change.product == 1
    assert odds_change.event_id == "sr:match:12345"

    assert [market] = odds_change.odds.market
    assert market.id == 1
    assert [o1, o2] = market.outcome
    assert o1.id == "1"
    assert Decimal.equal?(o1.odds, Decimal.new("1.85"))
    assert o2.id == "2"
  end

  test "decodes a product_down" do
    assert {:ok, %Feed.ProductDown{product: 1, timestamp: 123}} =
             Decoder.decode(~s(<product_down product="1" timestamp="123"/>))
  end

  test "returns an error for an unknown root element" do
    assert {:error, {:unknown_message, "nonsense"}} = Decoder.decode("<nonsense/>")
  end
end
