defmodule UOF.SDK.ProducerMonitor.Snapshot do
  @moduledoc """
  Atomic durable state owned by `UOF.SDK.ProducerMonitor`.

  `checkpoints` are incremental-recovery anchors, `resumable_producers` are the
  producer IDs allowed to drain retained backlog without immediate recovery,
  and `connection_tokens` are the committed upstream-session baselines.

  Restart resume requires both membership in `resumable_producers` and a
  checkpoint; either value alone is insufficient.
  """

  @type t :: %__MODULE__{
          checkpoints: %{optional(integer()) => integer()},
          resumable_producers: MapSet.t(integer()),
          connection_tokens: %{optional(atom()) => term()}
        }

  defstruct checkpoints: %{}, resumable_producers: MapSet.new(), connection_tokens: %{}

  def checkpoint(%__MODULE__{checkpoints: checkpoints}, id), do: Map.get(checkpoints, id)

  def put_checkpoint(%__MODULE__{} = snapshot, id, timestamp) do
    %{snapshot | checkpoints: Map.put(snapshot.checkpoints, id, timestamp)}
  end

  def resumable?(%__MODULE__{resumable_producers: producers}, id) do
    MapSet.member?(producers, id)
  end

  def mark_resumable(%__MODULE__{} = snapshot, id) do
    %{snapshot | resumable_producers: MapSet.put(snapshot.resumable_producers, id)}
  end

  def require_recovery(%__MODULE__{} = snapshot, id) when is_integer(id) do
    %{snapshot | resumable_producers: MapSet.delete(snapshot.resumable_producers, id)}
  end

  def require_recovery(%__MODULE__{} = snapshot, producer_ids) do
    Enum.reduce(producer_ids, snapshot, &require_recovery(&2, &1))
  end
end
