defmodule UOF.SDK.Context do
  @moduledoc """
  Metadata passed alongside every decoded message to a `UOF.SDK.MessageHandler`
  callback.

  Producer status is reported separately through `UOF.SDK.producer/1` and
  `UOF.SDK.MessageHandler.handle_producer_status/1`. Content delivery is
  independent of producer status.
  """

  @type t :: %__MODULE__{
          producer_id: integer() | nil,
          message_type: String.t() | nil,
          routing_key: String.t() | nil,
          event_urn: String.t() | nil
        }

  defstruct producer_id: nil,
            message_type: nil,
            routing_key: nil,
            event_urn: nil
end
