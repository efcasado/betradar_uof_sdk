defmodule UOF.SDK.AMQP.Error do
  @moduledoc false

  # BroadwayRabbitMQ's ChannelPool contract requires an exception on failure.
  # Keep the operation and original reason here for diagnostics, and classify
  # known transient failures consistently during checkout and channel setup.

  defexception [:operation, :reason]

  @impl true
  def message(%{operation: operation, reason: reason}) do
    "AMQP #{operation} failed: #{inspect(reason)}"
  end

  def retryable?(%__MODULE__{reason: reason}), do: retryable?(reason)

  def retryable?(reason)
      when reason in [
             :connecting,
             :closing,
             :closed,
             :econnrefused,
             :econnreset,
             :heartbeat_timeout,
             :enetunreach,
             :ehostunreach,
             :etimedout,
             :timeout,
             :unknown_host,
             :nxdomain,
             :noproc,
             :not_allowed
           ], do: true

  # Preserve Broadway's retry of an ambiguous disconnect during authentication.
  def retryable?({:auth_failure, ~c"Disconnected"}), do: true
  def retryable?({:socket_closed_unexpectedly, _}), do: true
  def retryable?({:server_initiated_close, 320, _}), do: true
  def retryable?({:shutdown, reason}), do: retryable?(reason)
  def retryable?({reason, {:gen_server, :call, _}}), do: retryable?(reason)
  def retryable?({reason, {GenServer, :call, _}}), do: retryable?(reason)
  def retryable?(_), do: false
end
