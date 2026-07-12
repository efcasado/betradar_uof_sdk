defmodule UOF.SDK.ProducerMonitor.Producer do
  @moduledoc """
  Runtime state reported for one Betradar producer.

  `status` is the complete lifecycle state:

    * `:down` — not synchronized and no recovery is in flight
    * `:recovering` — requesting or awaiting recovery completion
    * `:up` — synchronized and safe
    * `:delayed` — the remote feed is healthy but local processing is behind
    * `:resuming` — draining retained backlog after a restart and awaiting
      current-session confirmation
  """

  @type status :: :down | :recovering | :up | :delayed | :resuming

  @type t :: %__MODULE__{
          id: integer(),
          product: String.t() | nil,
          name: String.t() | nil,
          recovery_window_minutes: integer() | nil,
          status: status(),
          last_alive_at: integer() | nil,
          last_message_timestamp: integer() | nil,
          processing_queue_delay: integer() | nil
        }

  defstruct [
    :id,
    :product,
    :name,
    :recovery_window_minutes,
    :last_alive_at,
    :last_message_timestamp,
    :processing_queue_delay,
    status: :down
  ]

  @doc false
  @spec observe_alive(t(), integer()) :: t()
  def observe_alive(%__MODULE__{} = producer, now) do
    %{producer | last_alive_at: now}
  end

  @doc false
  @spec observe_message(t(), integer() | nil) :: t()
  def observe_message(%__MODULE__{} = producer, timestamp) do
    %{producer | last_message_timestamp: max_timestamp(producer.last_message_timestamp, timestamp)}
  end

  @doc false
  @spec start_recovery(t()) :: t()
  def start_recovery(%__MODULE__{} = producer) do
    %{producer | status: :recovering, processing_queue_delay: nil}
  end

  @doc false
  @spec complete_recovery(t(), integer()) :: t()
  def complete_recovery(%__MODULE__{} = producer, now) do
    %{producer | status: :up, last_alive_at: now, processing_queue_delay: nil}
  end

  @doc false
  @spec mark_delayed(t(), non_neg_integer()) :: t()
  def mark_delayed(%__MODULE__{} = producer, delay) do
    %{
      producer
      | status: :delayed,
        processing_queue_delay: delay
    }
  end

  @doc false
  @spec mark_up(t()) :: t()
  def mark_up(%__MODULE__{} = producer) do
    %{
      producer
      | status: :up,
        processing_queue_delay: nil
    }
  end

  defp max_timestamp(nil, timestamp), do: timestamp
  defp max_timestamp(timestamp, nil), do: timestamp
  defp max_timestamp(a, b), do: max(a, b)
end
