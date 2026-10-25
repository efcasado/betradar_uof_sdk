defmodule UOF.SDK.Pulsar do
  @moduledoc false

  # Derives the Pulsar client and the two Broadway producer specs from the
  # `:pulsar` configuration. Both subscriptions read one topic fed by the
  # RabbitMQ source connector; see the architecture guide's connector contract.

  alias OffBroadway.Pulsar.Producer
  alias Pulsar.Client

  @pulsar_client :uof_sdk_pulsar

  @type producer_spec :: {module(), keyword()}

  # Only the system pipeline observes feed continuity, so nothing confirms
  # that the content subscription exists before the first recovery. Pulsar
  # subscribes asynchronously and starts a new subscription at the latest
  # message by default: a content consumer still connecting would miss the
  # replay the system pipeline sees complete. Starting at the earliest
  # retained message lets it read what the system subscription has already
  # acknowledged, provided the topic retains acknowledged messages. Deployments
  # without retention create the subscriptions in advance instead; see the
  # deployment guide's subscriptions section.
  @content_consumer_defaults [initial_position: :earliest]

  # Processing is single-attempt. A failed message is nacked; without a
  # redelivery interval it would stay unacknowledged for the life of the
  # consumer, count against the broker's unacked-message limit, and return on
  # every restart. Instead it is redelivered once after a second and diverted,
  # before reaching the pipeline again, to the "<topic>-<subscription>-DLQ"
  # topic, where it stays inspectable. Batch-index acks keep one failure from
  # redelivering the messages that share its batch. Users may override any of
  # these.
  @pulsar_consumer_defaults [
    batch_index_ack_enabled: true,
    redelivery_interval: 1_000,
    dead_letter_policy: [max_redelivery: 1]
  ]

  @spec producers(keyword()) :: %{
          client: {module(), keyword()},
          content: producer_spec(),
          system: producer_spec()
        }
  def producers(opts) do
    host = Keyword.fetch!(opts, :host)
    topic = Keyword.fetch!(opts, :topic)
    subscription = Keyword.fetch!(opts, :subscription)

    client_opts =
      opts
      |> Keyword.get(:conn_opts, [])
      |> Keyword.merge(name: @pulsar_client, host: host)

    base_opts =
      opts
      |> Keyword.drop([:host, :topic, :topics, :conn_opts, :client])
      |> Keyword.put(:client, @pulsar_client)
      |> Keyword.put(:topics, [topic])

    # The system subscription is Failover, so the broker elects one instance
    # as its sole receiver. Ownership reports feed ProducerMonitor, which holds
    # control-plane authority (recovery issuance) only while active. The
    # Key_Shared content subscription never emits these reports.
    #
    # The callback is SDK-owned wiring, not a user extension point: a
    # user-supplied value would silently replace the monitor's ownership
    # signal and leave a demoted instance issuing recoveries forever.
    system_opts =
      Keyword.put(
        base_opts,
        :active_state_callback,
        {UOF.SDK.ProducerMonitor, :active_state_change, []}
      )

    %{
      # The client is supervised independently from both Broadway producers.
      # Neither producer carries :host, so both reuse this one connection
      # context instead of each calling Pulsar.start/1.
      client: {Client, client_opts},
      content: pulsar_producer(base_opts, subscription, :content, :key_shared, @content_consumer_defaults),
      system: pulsar_producer(system_opts, subscription, :system, :failover, [])
    }
  end

  defp pulsar_producer(opts, subscription, suffix, subscription_type, defaults) do
    consumer_opts =
      @pulsar_consumer_defaults
      |> Keyword.merge(defaults)
      |> Keyword.merge(Keyword.get(opts, :consumer_opts, []))
      |> Keyword.put(:subscription_type, subscription_type)

    opts =
      opts
      |> Keyword.put(:subscription, "#{subscription}-#{suffix}")
      |> Keyword.put(:consumer_opts, consumer_opts)

    {Producer, opts}
  end
end
