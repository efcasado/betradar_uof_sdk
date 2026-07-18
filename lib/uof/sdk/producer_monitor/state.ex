defmodule UOF.SDK.ProducerMonitor.State do
  @moduledoc false

  alias UOF.SDK.ProducerMonitor.Connections
  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Recovery
  alias UOF.SDK.ProducerMonitor.Snapshot

  @type ownership :: :always_active | {:failover, :active | :passive}

  @type t :: %__MODULE__{
          producers: %{optional(integer()) => Producer.t()},
          recoveries: %{optional(integer()) => Recovery.t()},
          last_recovery_at: %{optional(integer()) => integer()},
          snapshot: Snapshot.t(),
          connections: Connections.t(),
          ownership: ownership(),
          handler: module() | nil,
          now_fun: (-> integer()),
          monotonic_fun: (-> integer()),
          recover_fun: (term(), keyword() -> term()),
          monitor_store: module(),
          node_id: integer() | nil,
          gen_request_id: (-> integer()),
          inactivity_ms: non_neg_integer(),
          max_processing_delay_ms: non_neg_integer(),
          tick_ms: pos_integer(),
          min_interval_ms: non_neg_integer(),
          max_recovery_ms: pos_integer(),
          recovery_overlap_ms: non_neg_integer()
        }

  @enforce_keys [
    :producers,
    :recoveries,
    :last_recovery_at,
    :snapshot,
    :connections,
    :ownership,
    :handler,
    :now_fun,
    :monotonic_fun,
    :recover_fun,
    :monitor_store,
    :node_id,
    :gen_request_id,
    :inactivity_ms,
    :max_processing_delay_ms,
    :tick_ms,
    :min_interval_ms,
    :max_recovery_ms,
    :recovery_overlap_ms
  ]

  defstruct @enforce_keys
end
