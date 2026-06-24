defmodule UOF.SDK.ProducersTest do
  use ExUnit.Case, async: true

  alias UOF.Schemas.API.Descriptions.Producer, as: Desc
  alias UOF.SDK.Producer
  alias UOF.SDK.Producers

  test "builds producers from active descriptions, deriving product and window" do
    descriptions = [
      %Desc{
        id: 1,
        name: "LO",
        api_url: "https://api.betradar.com/v1/liveodds",
        active: true,
        stateful_recovery_window_in_minutes: 4320
      },
      %Desc{
        id: 3,
        name: "Ctrl",
        api_url: "https://api.betradar.com/v1/pre/",
        active: true,
        stateful_recovery_window_in_minutes: 4320
      },
      %Desc{id: 5, name: "Inactive", api_url: "https://api.betradar.com/v1/x", active: false}
    ]

    assert [%Producer{} = lo, %Producer{} = pre] = Producers.build(descriptions)

    assert lo.id == 1
    assert lo.product == "liveodds"
    assert lo.recovery_window_minutes == 4320
    assert lo.down? == true

    # trailing slash trimmed
    assert pre.product == "pre"
  end
end
