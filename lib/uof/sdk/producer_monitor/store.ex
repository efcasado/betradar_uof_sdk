defmodule UOF.SDK.ProducerMonitor.Store do
  @moduledoc """
  Persists connection and per-producer recovery state.

  Connection tokens carry a generation which advances atomically on a session
  change. Each producer records the generation in which it was last
  synchronized, so changing the connection generation invalidates every
  producer without a multi-record transaction.

  Each store has exactly one writer: its `UOF.SDK.ProducerMonitor`. Concurrent
  writes from another monitor, node, or administration tool are unsupported.
  """

  alias UOF.SDK.ProducerMonitor.Store.ConnectionState
  alias UOF.SDK.ProducerMonitor.Store.ProducerState

  @doc "Load the committed connection-token baseline."
  @callback load_connection_state() :: ConnectionState.t()

  @doc "Load durable state keyed by producer ID."
  @callback load_producer_states() :: %{optional(integer()) => ProducerState.t()}

  @doc "Atomically store the tokens and advance the connection generation."
  @callback commit_connection_change(%{optional(atom()) => term()}) :: ConnectionState.t()

  @doc "Monotonically advance one producer's recovery checkpoint."
  @callback advance_checkpoint(integer(), integer()) :: ProducerState.t()

  @doc "Durably prevent one producer from resuming without recovery."
  @callback require_recovery(integer()) :: ProducerState.t()

  @doc "Mark one producer synchronized in the given connection generation."
  @callback mark_synchronized(integer(), non_neg_integer()) :: ProducerState.t()

  @doc "Optional child specification for stores that own a process."
  @callback child_spec(term()) :: Supervisor.child_spec()

  @optional_callbacks child_spec: 1
end
