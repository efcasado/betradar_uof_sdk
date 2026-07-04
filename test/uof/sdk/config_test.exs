defmodule UOF.SDK.ConfigTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Config

  defp base_opts(extra \\ []) do
    Keyword.merge([handler: MyApp.Handler, access_token: "tok", host: "stgmq.betradar.com"], extra)
  end

  test "uses the configured host with sensible port/ssl defaults" do
    config = Config.load(base_opts())

    assert config.host == "stgmq.betradar.com"
    assert config.port == 5671
    assert config.ssl == true
    assert config.checkpoint_store == UOF.SDK.CheckpointStore.ETS
  end

  test "accepts an explicit port and ssl" do
    config = Config.load(base_opts(host: "localhost", port: 5672, ssl: false))
    assert config.host == "localhost"
    assert config.port == 5672
    assert config.ssl == false
  end

  test "accepts a custom checkpoint store" do
    config = Config.load(base_opts(checkpoint_store: MyApp.PgStore))
    assert config.checkpoint_store == MyApp.PgStore
  end

  test "raises on a missing required key" do
    assert_raise ArgumentError, ~r/:handler/, fn -> Config.load(access_token: "tok", host: "h") end
    assert_raise ArgumentError, ~r/:access_token/, fn -> Config.load(handler: X, host: "h") end
    assert_raise ArgumentError, ~r/:host/, fn -> Config.load(handler: X, access_token: "tok") end
  end

  describe "amqp_connection/1" do
    test "uses host/port, the access token as username, and TLS by default" do
      conn = base_opts() |> Config.load() |> Config.amqp_connection()

      assert conn[:host] == "stgmq.betradar.com"
      assert conn[:port] == 5671
      assert conn[:username] == "tok"
      assert conn[:password] == ""
      assert Keyword.has_key?(conn, :ssl_options)
    end

    test "raw :amqp options are merged in and win" do
      conn =
        [amqp: [ssl_options: [verify: :verify_none], heartbeat: 30]]
        |> base_opts()
        |> Config.load()
        |> Config.amqp_connection()

      assert conn[:ssl_options] == [verify: :verify_none]
      assert conn[:heartbeat] == 30
    end

    test "includes the virtual host when set and omits it otherwise" do
      with_vhost = [virtual_host: "/unifiedfeed/42"] |> base_opts() |> Config.load() |> Config.amqp_connection()
      assert with_vhost[:virtual_host] == "/unifiedfeed/42"

      without = base_opts() |> Config.load() |> Config.amqp_connection()
      refute Keyword.has_key?(without, :virtual_host)
    end

    test "omits ssl_options when ssl is disabled" do
      conn = [ssl: false] |> base_opts() |> Config.load() |> Config.amqp_connection()
      refute Keyword.has_key?(conn, :ssl_options)
    end
  end
end
