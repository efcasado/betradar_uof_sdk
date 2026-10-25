defmodule UOF.SDK.ProducerMonitor.Snapshot do
  @moduledoc """
  The producer monitor's persisted state and its binary encoding.

  `consumer_tag` holds the committed consumer tag of the source connector's
  upstream consume session.
  `producers` holds, per producer id, the recovery checkpoint (epoch
  milliseconds) and the recovery it still owes: `nil` when it is synchronized
  with that session, `:incremental` for a recovery from the checkpoint (or a
  full one without a checkpoint), and `:full` for a full recovery.
  A producer may resume retained backlog only when it owes no recovery and has
  a checkpoint.
  `descriptions` holds the last producer descriptions loaded from the UOF API,
  so the monitor can start when the API is unavailable.

  `UOF.SDK.ProducerMonitor.Store` implementations persist the encoded binary
  without interpreting it. `decode!/1` is public so operators can inspect a
  stored snapshot.
  """

  defmodule Progress do
    @moduledoc "Recovery progress for one producer."

    @type pending_recovery :: nil | :incremental | :full

    @type t :: %__MODULE__{checkpoint: integer() | nil, pending_recovery: pending_recovery()}

    # A producer with no saved progress owes a recovery.
    defstruct checkpoint: nil, pending_recovery: :incremental

    @doc false
    # A full recovery owed stays owed until it completes.
    @spec owe(t(), :incremental | :full) :: t()
    def owe(%__MODULE__{pending_recovery: :full} = progress, _kind), do: progress
    def owe(%__MODULE__{} = progress, kind), do: %{progress | pending_recovery: kind}
  end

  @type description :: %{
          id: integer(),
          name: String.t() | nil,
          product: String.t(),
          recovery_window_minutes: integer() | nil
        }

  @type t :: %__MODULE__{
          consumer_tag: binary() | nil,
          producers: %{optional(integer()) => Progress.t()},
          descriptions: [description()]
        }

  defstruct consumer_tag: nil, producers: %{}, descriptions: []

  @format :uof_sdk_monitor_snapshot
  @version 1

  @doc "Whether the producer may resume retained backlog without recovery."
  @spec resumable?(t(), integer()) :: boolean()
  def resumable?(%__MODULE__{} = snapshot, id) do
    match?(%Progress{pending_recovery: nil, checkpoint: checkpoint} when is_integer(checkpoint), progress(snapshot, id))
  end

  @doc false
  @spec progress(t(), integer()) :: Progress.t()
  def progress(%__MODULE__{producers: producers}, id), do: Map.get(producers, id, %Progress{})

  @doc false
  @spec put_progress(t(), integer(), Progress.t()) :: t()
  def put_progress(%__MODULE__{} = snapshot, id, %Progress{} = progress) do
    %{snapshot | producers: Map.put(snapshot.producers, id, progress)}
  end

  # A consume-session change can leave a delivery gap, so every producer owes a
  # recovery with the new consumer tag. Both changes are saved in one write.
  @doc false
  @spec change_session(t(), binary()) :: t()
  def change_session(%__MODULE__{} = snapshot, consumer_tag) do
    producers = Map.new(snapshot.producers, fn {id, progress} -> {id, Progress.owe(progress, :incremental)} end)
    %{snapshot | consumer_tag: consumer_tag, producers: producers}
  end

  @doc "Encode a snapshot for a store."
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{consumer_tag: consumer_tag, producers: producers, descriptions: descriptions}) do
    producers =
      Map.new(producers, fn {id, progress} ->
        {id, {progress.checkpoint, progress.pending_recovery}}
      end)

    :erlang.term_to_binary(
      {@format, @version, %{consumer_tag: consumer_tag, producers: producers, descriptions: descriptions}}
    )
  end

  @doc """
  Decode a stored snapshot. Raises `ArgumentError` if the binary is not a
  snapshot this SDK version can read.
  """
  @spec decode!(binary()) :: t()
  def decode!(binary) when is_binary(binary) do
    case binary_to_term(binary) do
      {@format, @version, %{consumer_tag: consumer_tag, producers: producers, descriptions: descriptions}}
      when (is_binary(consumer_tag) or is_nil(consumer_tag)) and is_map(producers) and is_list(descriptions) ->
        %__MODULE__{
          consumer_tag: consumer_tag,
          producers: Map.new(producers, &decode_progress!/1),
          descriptions: Enum.map(descriptions, &validate_description!/1)
        }

      {@format, @version, data} ->
        raise ArgumentError, "invalid producer monitor snapshot: #{inspect(data, limit: 5)}"

      {@format, version, _data} ->
        raise ArgumentError, "unsupported producer monitor snapshot version: #{inspect(version)}"

      other ->
        raise ArgumentError, "unreadable producer monitor snapshot: #{inspect(other, limit: 5)}"
    end
  end

  defp binary_to_term(binary) do
    :erlang.binary_to_term(binary, [:safe])
  rescue
    ArgumentError -> raise ArgumentError, "unreadable producer monitor snapshot: not a valid external term"
  end

  defp decode_progress!({id, {checkpoint, pending_recovery}})
       when is_integer(id) and (is_integer(checkpoint) or is_nil(checkpoint)) and
              pending_recovery in [nil, :incremental, :full] do
    {id, %Progress{checkpoint: checkpoint, pending_recovery: pending_recovery}}
  end

  defp decode_progress!(entry) do
    raise ArgumentError, "invalid producer monitor snapshot progress: #{inspect(entry)}"
  end

  defp validate_description!(%{id: id, name: name, product: product, recovery_window_minutes: window} = description)
       when is_integer(id) and (is_binary(name) or is_nil(name)) and is_binary(product) and
              (is_integer(window) or is_nil(window)) and map_size(description) == 4 do
    description
  end

  defp validate_description!(entry) do
    raise ArgumentError, "invalid producer monitor snapshot description: #{inspect(entry)}"
  end
end
