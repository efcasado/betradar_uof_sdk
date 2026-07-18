defmodule UOF.SDK.ProducerMonitor.Health do
  @moduledoc false

  alias UOF.SDK.ProducerMonitor.Producer

  @type alive_result ::
          {:ok, Producer.t()}
          | {:checkpoint, Producer.t(), integer()}
          | {:recovery_needed, Producer.t(), :initial_sync | :unsubscribed}

  @type check_result ::
          :unchanged
          | {:transition, Producer.t()}
          | {:recovery_needed, Producer.t(), :alive_timeout}

  @spec observe_alive(Producer.t(), integer() | nil, boolean(), integer(), boolean()) :: alive_result()
  def observe_alive(producer, gen_timestamp, subscribed?, now, recovering?) do
    observed = Producer.observe_alive(producer, now)

    cond do
      recovering? ->
        {:ok, observed}

      not subscribed? ->
        {:recovery_needed, observed, :unsubscribed}

      producer.status == :down ->
        {:recovery_needed, observed, :initial_sync}

      producer.status == :up and is_integer(gen_timestamp) ->
        {:checkpoint, observed, gen_timestamp}

      true ->
        {:ok, observed}
    end
  end

  @spec observe_message(Producer.t(), integer() | nil) :: Producer.t()
  def observe_message(producer, gen_timestamp) do
    Producer.observe_message(producer, gen_timestamp)
  end

  @spec check(Producer.t(), integer(), non_neg_integer(), non_neg_integer(), boolean()) :: check_result()
  def check(producer, now, inactivity_ms, max_processing_delay_ms, connections_ready?) do
    cond do
      alive_violation?(producer, now, inactivity_ms) ->
        {:recovery_needed, producer, :alive_timeout}

      producer.status == :up and processing_violation?(producer, now, max_processing_delay_ms) ->
        {:transition, Producer.mark_delayed(producer, now - producer.last_message_timestamp)}

      producer.status == :delayed and
          not processing_violation?(producer, now, max_processing_delay_ms) ->
        {:transition, Producer.mark_up(producer)}

      producer.status == :resuming and producer.last_alive_at != nil and connections_ready? and
          not processing_violation?(producer, now, max_processing_delay_ms) ->
        {:transition, Producer.mark_up(producer)}

      true ->
        :unchanged
    end
  end

  defp alive_violation?(%Producer{last_alive_at: nil}, _now, _ms), do: false
  defp alive_violation?(%Producer{last_alive_at: timestamp}, now, ms), do: now - timestamp > ms

  # Content timestamps include event messages and content-session alives. A
  # fresh system alive is intentionally not evidence that content caught up.
  defp processing_violation?(%Producer{last_message_timestamp: nil}, _now, _ms), do: false

  defp processing_violation?(%Producer{last_message_timestamp: timestamp}, now, ms), do: now - timestamp > ms
end
