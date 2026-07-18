defmodule UOF.SDK.ProducerMonitor.Recovery do
  @moduledoc false

  @type t :: %__MODULE__{
          after_ts: integer() | nil,
          request_id: integer() | nil,
          generation: reference()
        }

  defstruct [:after_ts, :request_id, :generation]

  def new(after_ts) do
    %__MODULE__{after_ts: after_ts, generation: make_ref()}
  end

  def pending(%__MODULE__{} = recovery) do
    %{recovery | request_id: nil, generation: make_ref()}
  end

  def pending(%__MODULE__{} = recovery, after_ts) do
    %{recovery | after_ts: after_ts, request_id: nil, generation: make_ref()}
  end

  def in_flight(%__MODULE__{} = recovery, request_id) do
    %{recovery | request_id: request_id}
  end

  def pending?(%__MODULE__{request_id: nil}), do: true
  def pending?(%__MODULE__{}), do: false

  def matches_generation?(%__MODULE__{request_id: nil, generation: generation}, generation), do: true
  def matches_generation?(%__MODULE__{}, _generation), do: false

  def matches_request?(%__MODULE__{request_id: request_id}, request_id) when not is_nil(request_id), do: true
  def matches_request?(%__MODULE__{}, _request_id), do: false
end
