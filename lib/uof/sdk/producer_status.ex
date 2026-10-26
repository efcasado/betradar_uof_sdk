defmodule UOF.SDK.ProducerStatus do
  @moduledoc """
  A producer's reported state, returned by `UOF.SDK.producers/0` and
  `UOF.SDK.producer/1` and passed to `c:UOF.SDK.MessageHandler.handle_producer_status/1`.

  `status` is one of:

    * `:down` — not synchronized; the recovery it owes waits for the
      producer's alives
    * `:recovering` — waiting to request, requesting, or awaiting recovery
      completion
    * `:up` — the topic carries a continuous feed for the producer; content
      processing may still be catching up
    * `:resuming` — draining retained backlog after a restart and awaiting
      current-session confirmation

  `last_alive_at` is when this node last observed a system heartbeat, in epoch
  milliseconds. `alive_delay` is how late that heartbeat was read, in
  milliseconds: its observation time minus its generation timestamp. It shows
  how far behind the system pipeline reads the topic, plus any clock skew with
  Betradar.
  """

  @type status :: :down | :recovering | :up | :resuming

  @type t :: %__MODULE__{
          id: integer(),
          name: String.t() | nil,
          product: String.t() | nil,
          recovery_window_minutes: integer() | nil,
          status: status(),
          last_alive_at: integer() | nil,
          alive_delay: integer() | nil
        }

  @enforce_keys [:id, :status]
  defstruct [:id, :name, :product, :recovery_window_minutes, :status, :last_alive_at, :alive_delay]
end
