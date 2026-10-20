defmodule UOF.SDK.MessageHandler do
  @moduledoc """
  Behaviour implemented by user applications to receive decoded feed messages.

  `use UOF.SDK.MessageHandler` injects a no-op default for every callback, so
  implementations only override the messages they care about:

      defmodule MyApp.FeedHandler do
        use UOF.SDK.MessageHandler

        @impl true
        def handle_odds_change(odds_change, ctx) do
          # ...
          :ok
        end
      end

  Each callback receives the decoded `UOF.Schemas.Feed.*` struct (from
  `uof_schemas`) and a `UOF.SDK.Context` describing the producer, scope and
  routing key.
  """

  alias UOF.Schemas.Feed
  alias UOF.SDK.Context

  @content_message_types ~w[
    odds_change
    bet_settlement
    bet_stop
    bet_cancel
    rollback_bet_cancel
    rollback_bet_settlement
    fixture_change
  ]

  @callback handle_odds_change(%Feed.OddsChange{}, Context.t()) :: :ok
  @callback handle_bet_settlement(%Feed.BetSettlement{}, Context.t()) :: :ok
  @callback handle_bet_stop(%Feed.BetStop{}, Context.t()) :: :ok
  @callback handle_bet_cancel(%Feed.BetCancel{}, Context.t()) :: :ok
  @callback handle_rollback_bet_cancel(%Feed.RollbackBetCancel{}, Context.t()) :: :ok
  @callback handle_rollback_bet_settlement(%Feed.RollbackBetSettlement{}, Context.t()) :: :ok
  @callback handle_fixture_change(%Feed.FixtureChange{}, Context.t()) :: :ok

  @doc """
  Called whenever a producer's status changes (up, down, recovering, resuming). The
  same `UOF.SDK.ProducerStatus` returned by `UOF.SDK.producers/0`.
  """
  @callback handle_producer_status(UOF.SDK.ProducerStatus.t()) :: :ok

  # Routing-key message types delivered to the content callbacks above.
  @doc false
  @spec content_message_types() :: [String.t()]
  def content_message_types, do: @content_message_types

  defmacro __using__(_opts) do
    quote do
      @behaviour UOF.SDK.MessageHandler

      @impl true
      def handle_odds_change(_message, _context), do: :ok
      @impl true
      def handle_bet_settlement(_message, _context), do: :ok
      @impl true
      def handle_bet_stop(_message, _context), do: :ok
      @impl true
      def handle_bet_cancel(_message, _context), do: :ok
      @impl true
      def handle_rollback_bet_cancel(_message, _context), do: :ok
      @impl true
      def handle_rollback_bet_settlement(_message, _context), do: :ok
      @impl true
      def handle_fixture_change(_message, _context), do: :ok
      @impl true
      def handle_producer_status(_producer), do: :ok

      defoverridable handle_odds_change: 2,
                     handle_bet_settlement: 2,
                     handle_bet_stop: 2,
                     handle_bet_cancel: 2,
                     handle_rollback_bet_cancel: 2,
                     handle_rollback_bet_settlement: 2,
                     handle_fixture_change: 2,
                     handle_producer_status: 1
    end
  end
end
