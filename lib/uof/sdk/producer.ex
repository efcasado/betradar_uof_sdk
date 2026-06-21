defmodule UOF.SDK.Producer do
  @moduledoc """
  Runtime state of a single Betradar producer.

  Producers start **down** (the feed must recover to get in sync before bets on
  that producer's markets are safe). The fields mirror the producer-status
  callback the SDK delivers, so the registry and the callback share one shape.

  `down?`/`delayed?` are the two independent "not healthy" axes:

    * `down?` due to **delivery/connection** issues triggers recovery.
    * `down?` due to **slow consumer processing** (`delayed?`) does *not* — the
      producer keeps delivering and recovers when processing catches up.

  Timestamps are milliseconds since the Unix epoch (the feed's unit).
  """

  @typedoc """
  Why the producer last changed state, mirroring Betradar's `ProducerStatusReason`.
  """
  @type reason ::
          :first_recovery_completed
          | :processing_queue_delay_violation
          | :processing_queue_delay_stabilized
          | :alive_interval_violation
          | :connection_down
          | :returned_from_inactivity
          | :other
          | nil

  @type t :: %__MODULE__{
          id: integer(),
          product: String.t() | nil,
          name: String.t() | nil,
          recovery_window_minutes: integer() | nil,
          down?: boolean(),
          delayed?: boolean(),
          reason: reason(),
          recovering?: boolean(),
          recovery_id: integer() | nil,
          last_alive_at: integer() | nil,
          last_message_timestamp: integer() | nil,
          last_processed_message_gen_timestamp: integer() | nil,
          processing_queue_delay: integer() | nil
        }

  defstruct [
    :id,
    :product,
    :name,
    :recovery_window_minutes,
    :reason,
    :recovery_id,
    :last_alive_at,
    :last_message_timestamp,
    :last_processed_message_gen_timestamp,
    :processing_queue_delay,
    down?: true,
    delayed?: false,
    recovering?: false
  ]

  @doc "Whether the producer is up (in sync, safe to accept bets)."
  @spec up?(t()) :: boolean()
  def up?(%__MODULE__{down?: down?}), do: not down?
end
