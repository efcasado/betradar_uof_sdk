defmodule UOF.SDK.ProducerMonitor.Store.ConnectionState do
  @moduledoc """
  Durable connection-token baseline for a producer monitor.

  `generation` advances whenever either consume-session token changes. Producer
  state synchronized against an older generation is not eligible to resume.
  """

  @type t :: %__MODULE__{
          tokens: %{optional(atom()) => term()},
          generation: non_neg_integer()
        }

  defstruct tokens: %{}, generation: 0
end
