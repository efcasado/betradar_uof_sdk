defmodule UOF.SDK.MessageHandlerTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.MessageHandler

  test "every content message type has a content callback" do
    callbacks = MessageHandler.behaviour_info(:callbacks)

    for type <- MessageHandler.content_message_types() do
      assert {String.to_atom("handle_" <> type), 2} in callbacks
    end
  end
end
