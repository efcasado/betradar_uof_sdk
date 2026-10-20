defmodule UOF.SDK.ProducerStatus do
  @moduledoc """
  A producer's reported state, returned by `UOF.SDK.producers/0` and
  `UOF.SDK.producer/1` and passed to `c:UOF.SDK.MessageHandler.handle_producer_status/1`.

  `status` is one of:

    * `:down` — not synchronized and no recovery is pending
    * `:recovering` — waiting to request, requesting, or awaiting recovery
      completion
    * `:up` — synchronized: the feed has no known gap. Content, including a
      recovery replay, may still be awaiting processing; measure processing
      lag from the feed timestamps your callbacks receive.
    * `:resuming` — draining retained backlog after a restart and awaiting
      current-session confirmation

  `last_alive_at` is when this node last observed a system heartbeat, in epoch
  milliseconds.
  """

  @type status :: :down | :recovering | :up | :resuming

  @type t :: %__MODULE__{
          id: integer(),
          name: String.t() | nil,
          product: String.t() | nil,
          recovery_window_minutes: integer() | nil,
          status: status(),
          last_alive_at: integer() | nil
        }

  @enforce_keys [:id, :status]
  defstruct [
    :id,
    :name,
    :product,
    :recovery_window_minutes,
    :status,
    :last_alive_at
  ]
end
