defmodule UOF.SDK.ProducerMonitor.Store.PostgresTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.ProducerMonitor.Store.Postgres

  @moduletag :postgres
  @moduletag skip: is_nil(System.get_env("UOF_TEST_POSTGRES_URL"))

  setup do
    case System.get_env("UOF_TEST_POSTGRES_URL") do
      nil ->
        {:ok, skip: "set UOF_TEST_POSTGRES_URL to run PostgreSQL store tests"}

      url ->
        {:ok, conn} = Postgrex.start_link(url: url)
        Postgrex.query!(conn, """
        CREATE TABLE IF NOT EXISTS uof_sdk_monitor_snapshots (
          subscription text PRIMARY KEY,
          snapshot bytea NOT NULL
        )
        """, [])

        subscription = "test-#{System.unique_integer([:positive])}"
        on_exit(fn ->
          {:ok, cleanup} = Postgrex.start_link(url: url)
          Postgrex.query!(cleanup, "DELETE FROM uof_sdk_monitor_snapshots WHERE subscription = $1", [subscription])
          GenServer.stop(cleanup)
        end)

        {:ok, opts: [conn: conn, subscription: subscription]}
    end
  end

  test "loads nil, persists and replaces the opaque snapshot", %{opts: opts} do
    assert Postgres.load(opts) == nil
    assert :ok = Postgres.save(<<0, 1, 255>>, opts)
    assert Postgres.load(opts) == <<0, 1, 255>>
    assert :ok = Postgres.save(<<2, 3>>, opts)
    assert Postgres.load(opts) == <<2, 3>>
  end

  test "backend errors are visible", %{opts: opts} do
    GenServer.stop(Keyword.fetch!(opts, :conn))
    assert catch_exit(Postgres.load(opts))
  end
end
