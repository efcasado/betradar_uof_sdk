defmodule UOF.SDK.LogHandler do
  @moduledoc """
  A built-in `UOF.SDK.MessageHandler` that logs every message it receives.

  Handy for smoke tests and first connections — point the SDK at it instead of
  writing your own handler:

      UOF.SDK.start_link(handler: UOF.SDK.LogHandler, access_token: "...", ...)
  """

  use UOF.SDK.MessageHandler

  require Logger

  @impl true
  def handle_odds_change(msg, ctx), do: log("odds_change", msg, ctx)
  @impl true
  def handle_bet_settlement(msg, ctx), do: log("bet_settlement", msg, ctx)
  @impl true
  def handle_bet_stop(msg, ctx), do: log("bet_stop", msg, ctx)
  @impl true
  def handle_bet_cancel(msg, ctx), do: log("bet_cancel", msg, ctx)
  @impl true
  def handle_rollback_bet_cancel(msg, ctx), do: log("rollback_bet_cancel", msg, ctx)
  @impl true
  def handle_rollback_bet_settlement(msg, ctx), do: log("rollback_bet_settlement", msg, ctx)
  @impl true
  def handle_fixture_change(msg, ctx), do: log("fixture_change", msg, ctx)
  @impl true
  def handle_producer_status(producer) do
    Logger.info(
      "UOF.SDK producer #{producer.id} #{if producer.down?, do: "DOWN", else: "UP"} " <>
        "delayed=#{producer.delayed?} reason=#{inspect(producer.reason)}"
    )

    :ok
  end

  defp log(type, _msg, ctx) do
    Logger.info(
      "UOF.SDK #{type} producer=#{inspect(ctx.producer_id)} " <>
        "event=#{inspect(ctx.event_urn)} rk=#{ctx.routing_key}"
    )

    :ok
  end
end
