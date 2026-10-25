defmodule UOF.SDK.ProducerMonitor.Store do
  @moduledoc """
  Persists the producer monitor's state as one opaque binary snapshot.

  The monitor owns the snapshot's contents and encoding
  (`UOF.SDK.ProducerMonitor.Snapshot`); a store only keeps the latest binary.
  Configure a store as `module` or `{module, opts}`; `opts` is passed to every
  callback.

  A store keeps one snapshot. All SDK instances of a deployment use the same
  store and therefore share it: only the instance that owns the system
  subscription writes it, and a promoted instance reloads it.

  Let backend failures raise or exit. Returning `nil` for a failed read, or
  `:ok` for a failed write, would give the monitor an incorrect recovery
  baseline.
  """

  @doc """
  Return the saved snapshot, or `nil` when none has been saved.
  """
  @callback load(opts :: keyword()) :: binary() | nil

  @doc """
  Atomically replace the saved snapshot. Return `:ok` only after the write is
  durable.
  """
  @callback save(snapshot :: binary(), opts :: keyword()) :: :ok

  @doc "Optional child specification for stores that own a process."
  @callback child_spec(opts :: keyword()) :: Supervisor.child_spec()

  @optional_callbacks child_spec: 1
end
