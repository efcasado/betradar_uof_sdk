defmodule UOF.SDK.Context do
  @moduledoc """
  Metadata passed alongside every decoded message to a `UOF.SDK.MessageHandler`
  callback.

  `status` reflects the producer's recovery state (`:up`, `:recovering`,
  `:down`, ...). Until the producer lifecycle layer lands it is `:unknown`.
  """

  @type status :: :unknown | :up | :recovering | :down

  @type t :: %__MODULE__{
          producer_id: integer() | nil,
          message_type: String.t() | nil,
          routing_key: String.t() | nil,
          event_urn: String.t() | nil,
          status: status()
        }

  defstruct producer_id: nil,
            message_type: nil,
            routing_key: nil,
            event_urn: nil,
            status: :unknown
end
