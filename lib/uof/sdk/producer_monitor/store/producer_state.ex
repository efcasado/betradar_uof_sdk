defmodule UOF.SDK.ProducerMonitor.Store.ProducerState do
  @moduledoc """
  Durable recovery state for one producer.

  A producer may resume retained backlog only when it has a checkpoint and its
  `synchronized_generation` matches the current connection generation.
  """

  @type t :: %__MODULE__{
          checkpoint: integer() | nil,
          synchronized_generation: non_neg_integer() | nil
        }

  defstruct checkpoint: nil, synchronized_generation: nil

  def resumable?(%__MODULE__{checkpoint: checkpoint, synchronized_generation: generation}, generation)
      when is_integer(checkpoint), do: true

  def resumable?(%__MODULE__{}, _generation), do: false
end
