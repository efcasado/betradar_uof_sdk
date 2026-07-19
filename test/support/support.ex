defmodule UOF.SDK.TestSupport do
  @moduledoc false

  @pulsar_http_port System.get_env("PULSAR_HTTP_PORT", "18080")
  @rabbitmq_http_port System.get_env("RABBITMQ_HTTP_PORT", "25672")
  @source_restart_attempts 300
  @poll_interval_ms 100

  def start_http_client! do
    {:ok, _apps} = Application.ensure_all_started(:inets)
    :ok
  end

  def restart_source! do
    url =
      ~c"http://localhost:#{@pulsar_http_port}/admin/v3/sources/public/default/uof-rabbitmq/restart"

    case :httpc.request(:post, {url, [], ~c"application/json", ~c""}, [timeout: 10_000], []) do
      {:ok, {{_version, status, _reason}, _headers, _body}} when status in [200, 204] ->
        :ok

      result ->
        raise "could not restart Pulsar source: #{inspect(result)}"
    end
  end

  def wait_for_new_source_queue!(old_queue, attempts \\ @source_restart_attempts)

  def wait_for_new_source_queue!(old_queue, attempts) when attempts > 0 do
    case source_queue() do
      {:ok, queue} when queue != old_queue ->
        queue

      _result ->
        Process.sleep(@poll_interval_ms)
        wait_for_new_source_queue!(old_queue, attempts - 1)
    end
  end

  def wait_for_new_source_queue!(old_queue, 0) do
    timeout_ms = @source_restart_attempts * @poll_interval_ms

    raise "RabbitMQ source queue did not change from #{old_queue} within #{timeout_ms}ms"
  end

  def wait_for_subscription!(subscription, attempts \\ 100)

  def wait_for_subscription!(subscription, attempts) when attempts > 0 do
    url =
      ~c"http://localhost:#{@pulsar_http_port}/admin/v2/persistent/public/default/uof-feed/subscriptions"

    case :httpc.request(:get, {url, []}, [timeout: 1_000], body_format: :binary) do
      {:ok, {{_version, 200, _reason}, _headers, body}} ->
        if String.contains?(body, subscription) do
          :ok
        else
          Process.sleep(100)
          wait_for_subscription!(subscription, attempts - 1)
        end

      _result ->
        Process.sleep(100)
        wait_for_subscription!(subscription, attempts - 1)
    end
  end

  def wait_for_subscription!(subscription, 0) do
    raise "Pulsar subscription #{subscription} did not become ready"
  end

  def source_queue do
    url = ~c"http://localhost:#{@rabbitmq_http_port}/api/queues/%2f"
    authorization = ~c"Basic #{Base.encode64("guest:guest")}"

    case :httpc.request(:get, {url, [{~c"authorization", authorization}]}, [timeout: 1_000], body_format: :binary) do
      {:ok, {{_version, 200, _reason}, _headers, body}} ->
        body
        |> Jason.decode!()
        |> Enum.find_value(:not_ready, fn
          %{"consumers" => consumers, "name" => name} when consumers > 0 -> {:ok, name}
          _queue -> nil
        end)

      result ->
        result
    end
  end
end
