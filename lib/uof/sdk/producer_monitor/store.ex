defmodule UOF.SDK.ProducerMonitor.Store do
  @moduledoc """
  Persists transport-session and per-producer recovery progress.

  Session tokens carry a generation which advances atomically on a consume
  session change. Each producer records the generation in which it was last
  synchronized, so changing the session generation invalidates every producer
  without a multi-record transaction.

  Each store has exactly one writer: its `UOF.SDK.ProducerMonitor`. Concurrent
  writes from another monitor, node, or administration tool are unsupported.
  """

  defmodule Session do
    @moduledoc """
    Durable consume-session baseline for a producer monitor.

    `generation` advances whenever either the system or content consume-session
    token changes. Producer progress synchronized against an older generation
    is not eligible to resume.
    """

    @type t :: %__MODULE__{
            tokens: %{optional(atom()) => term()},
            generation: non_neg_integer()
          }

    defstruct tokens: %{}, generation: 0
  end

  defmodule ProducerProgress do
    @moduledoc """
    Durable recovery progress for one producer.

    A producer may resume retained backlog only when it has a checkpoint and
    its `synchronized_generation` matches the current session generation.
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

  @doc "Load the committed consume-session baseline."
  @callback load_session() :: Session.t()

  @doc "Load durable progress keyed by producer ID."
  @callback load_producer_progress() :: %{optional(integer()) => ProducerProgress.t()}

  @doc "Atomically store the tokens and advance the session generation."
  @callback commit_session_change(%{optional(atom()) => term()}) :: Session.t()

  @doc "Monotonically advance one producer's recovery checkpoint."
  @callback advance_checkpoint(integer(), integer()) :: ProducerProgress.t()

  @doc "Durably prevent one producer from resuming without recovery."
  @callback require_recovery(integer()) :: ProducerProgress.t()

  @doc "Mark one producer synchronized in the given session generation."
  @callback mark_synchronized(integer(), non_neg_integer()) :: ProducerProgress.t()

  @doc "Optional child specification for stores that own a process."
  @callback child_spec(term()) :: Supervisor.child_spec()

  @optional_callbacks child_spec: 1
end
