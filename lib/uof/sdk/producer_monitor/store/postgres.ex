defmodule UOF.SDK.ProducerMonitor.Store.Postgres do
  @moduledoc """
  PostgreSQL-backed monitor snapshot store.

  Uses an existing `Postgrex` connection or pool supplied through `:conn`.
  The application must start that connection before the SDK and create the
  `uof_sdk_monitor_snapshots` table (see `docs/deployment.md`).

  `:subscription` identifies the logical SDK subscription shared by all
  instances, not an individual instance.
  """

  @behaviour UOF.SDK.ProducerMonitor.Store

  @impl true
  def load(opts) do
    conn = Keyword.fetch!(opts, :conn)
    subscription = Keyword.fetch!(opts, :subscription)
    timeout = Keyword.get(opts, :timeout, 2_000)

    case Postgrex.query!(conn, "SELECT snapshot FROM uof_sdk_monitor_snapshots WHERE subscription = $1", [
           subscription
         ], timeout: timeout).rows do
      [] -> nil
      [[snapshot]] -> snapshot
    end
  end

  @impl true
  def save(snapshot, opts) when is_binary(snapshot) do
    conn = Keyword.fetch!(opts, :conn)
    subscription = Keyword.fetch!(opts, :subscription)
    timeout = Keyword.get(opts, :timeout, 2_000)

    Postgrex.query!(
      conn,
      """
      INSERT INTO uof_sdk_monitor_snapshots (subscription, snapshot)
      VALUES ($1, $2)
      ON CONFLICT (subscription) DO UPDATE SET snapshot = EXCLUDED.snapshot
      """,
      [subscription, snapshot],
      timeout: timeout
    )

    :ok
  end
end
