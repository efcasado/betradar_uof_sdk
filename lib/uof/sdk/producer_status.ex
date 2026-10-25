defmodule UOF.SDK.ProducerStatus do
  @moduledoc """
  A producer's reported state, returned by `UOF.SDK.producers/0` and
  `UOF.SDK.producer/1` and passed to `c:UOF.SDK.MessageHandler.handle_producer_status/1`.

  `status` is one of:

    * `:down` — not synchronized and no recovery is pending
    * `:recovering` — waiting to request, requesting, or awaiting recovery
      completion
    * `:up` — synchronized
    * `:delayed` — the remote feed is healthy but local processing is behind
      (AMQP only; the Pulsar transport does not track content lag)
    * `:resuming` — draining retained backlog after a restart and awaiting
      current-session confirmation

  Timestamps are epoch milliseconds. `last_alive_at` is when this node last
  observed a system heartbeat; `last_message_timestamp` is the newest processed
  feed timestamp, and is `nil` with the Pulsar transport. `processing_queue_delay`
  is set only while `:delayed`.
  """

  @type status :: :down | :recovering | :up | :delayed | :resuming

  @type t :: %__MODULE__{
          id: integer(),
          name: String.t() | nil,
          product: String.t() | nil,
          recovery_window_minutes: integer() | nil,
          status: status(),
          last_alive_at: integer() | nil,
          last_message_timestamp: integer() | nil,
          processing_queue_delay: integer() | nil
        }

  @enforce_keys [:id, :status]
  defstruct [
    :id,
    :name,
    :product,
    :recovery_window_minutes,
    :status,
    :last_alive_at,
    :last_message_timestamp,
    :processing_queue_delay
  ]
end
