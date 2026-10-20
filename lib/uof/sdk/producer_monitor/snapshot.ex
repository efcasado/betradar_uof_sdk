defmodule UOF.SDK.ProducerMonitor.Snapshot do
  @moduledoc """
  The producer monitor's persisted state and its binary encoding.

  `tokens` holds the committed system and content consume-session tokens.
  `producers` holds, per producer id, the recovery checkpoint (epoch
  milliseconds) and whether the producer is synchronized with those sessions.
  A producer may resume retained backlog only when it is synchronized and has a
  checkpoint.

  `UOF.SDK.ProducerMonitor.Store` implementations persist the encoded binary
  without interpreting it. `decode!/1` is public so operators can inspect a
  stored snapshot.
  """

  defmodule Progress do
    @moduledoc "Recovery progress for one producer."

    @type t :: %__MODULE__{checkpoint: integer() | nil, synchronized: boolean()}

    defstruct checkpoint: nil, synchronized: false
  end

  @type t :: %__MODULE__{
          tokens: %{optional(:system | :content) => binary()},
          producers: %{optional(integer()) => Progress.t()}
        }

  defstruct tokens: %{}, producers: %{}

  @format :uof_sdk_monitor_snapshot
  @version 1
  @namespaces [:system, :content]

  @doc "Whether the producer may resume retained backlog without recovery."
  @spec resumable?(t(), integer()) :: boolean()
  def resumable?(%__MODULE__{} = snapshot, id) do
    match?(%Progress{synchronized: true, checkpoint: checkpoint} when is_integer(checkpoint), progress(snapshot, id))
  end

  @doc false
  @spec progress(t(), integer()) :: Progress.t()
  def progress(%__MODULE__{producers: producers}, id), do: Map.get(producers, id, %Progress{})

  @doc false
  @spec put_progress(t(), integer(), Progress.t()) :: t()
  def put_progress(%__MODULE__{} = snapshot, id, %Progress{} = progress) do
    %{snapshot | producers: Map.put(snapshot.producers, id, progress)}
  end

  # A consume-session change can leave a delivery gap, so no producer remains
  # synchronized with the new tokens. Both changes are saved in one write.
  @doc false
  @spec change_session(t(), map()) :: t()
  def change_session(%__MODULE__{} = snapshot, tokens) do
    producers = Map.new(snapshot.producers, fn {id, progress} -> {id, %{progress | synchronized: false}} end)
    %{snapshot | tokens: tokens, producers: producers}
  end

  @doc "Encode a snapshot for a store."
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{tokens: tokens, producers: producers}) do
    producers = Map.new(producers, fn {id, progress} -> {id, {progress.checkpoint, progress.synchronized}} end)
    :erlang.term_to_binary({@format, @version, %{tokens: tokens, producers: producers}})
  end

  @doc """
  Decode a stored snapshot. Raises `ArgumentError` if the binary is not a
  snapshot this SDK version can read.
  """
  @spec decode!(binary()) :: t()
  def decode!(binary) when is_binary(binary) do
    case binary_to_term(binary) do
      {@format, @version, %{tokens: tokens, producers: producers}} when is_map(tokens) and is_map(producers) ->
        %__MODULE__{tokens: validate_tokens!(tokens), producers: Map.new(producers, &decode_progress!/1)}

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

  defp validate_tokens!(tokens) do
    Enum.each(tokens, fn
      {namespace, token} when namespace in @namespaces and is_binary(token) -> :ok
      entry -> raise ArgumentError, "invalid producer monitor snapshot token: #{inspect(entry)}"
    end)

    tokens
  end

  defp decode_progress!({id, {checkpoint, synchronized}})
       when is_integer(id) and (is_integer(checkpoint) or is_nil(checkpoint)) and is_boolean(synchronized) do
    {id, %Progress{checkpoint: checkpoint, synchronized: synchronized}}
  end

  defp decode_progress!(entry) do
    raise ArgumentError, "invalid producer monitor snapshot progress: #{inspect(entry)}"
  end
end
