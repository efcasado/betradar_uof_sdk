defmodule UOF.SDK.ProducerMonitor.Store do
  @moduledoc """
  Persists the producer monitor's state as one coherent snapshot.

  Saving checkpoints, resumable producer IDs, and connection tokens
  atomically prevents a restart from combining values from different logical
  transitions.

  A snapshot has exactly one writer: its `UOF.SDK.ProducerMonitor`. Concurrent
  writes from another monitor, node, or administration tool are unsupported.
  """

  alias UOF.SDK.ProducerMonitor.Snapshot

  @doc "Load the complete snapshot when its producer monitor starts."
  @callback load() :: Snapshot.t()

  @doc """
  Atomically replace the complete snapshot.

  All fields must become visible as one unit; splitting this write across
  non-transactional operations can cause recovery to be skipped after a crash.
  """
  @callback save(Snapshot.t()) :: :ok

  @doc "Optional child specification for stores that own a process."
  @callback child_spec(term()) :: Supervisor.child_spec()

  @optional_callbacks child_spec: 1
end
