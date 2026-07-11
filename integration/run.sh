#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
connectors_dir="${PULSAR_CONNECTORS_DIR:-${root_dir}/../pulsar-connectors}"
compose=(docker compose --project-directory "${root_dir}/integration" --file "${root_dir}/integration/docker-compose.yml")
if command -v mix >/dev/null 2>&1; then
  mix_command=(mix)
else
  mix_command=(mise exec -- mix)
fi

"${connectors_dir}/gradlew" -p "${connectors_dir}" :rabbitmq:assemble

connector_nar="$(find "${connectors_dir}/rabbitmq/build/libs" -maxdepth 1 -name 'rabbitmq-*.nar' -print -quit)"
if [[ -z "${connector_nar}" ]]; then
  echo "RabbitMQ connector NAR was not produced" >&2
  exit 1
fi

export RABBITMQ_CONNECTOR_NAR="${connector_nar}"
export RABBITMQ_AMQP_PORT="${RABBITMQ_AMQP_PORT:-15672}"
export RABBITMQ_HTTP_PORT="${RABBITMQ_HTTP_PORT:-25672}"
export PULSAR_BROKER_PORT="${PULSAR_BROKER_PORT:-16650}"
export PULSAR_HTTP_PORT="${PULSAR_HTTP_PORT:-18080}"

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans
}
trap cleanup EXIT

"${compose[@]}" up --detach --wait

curl --fail --silent --show-error \
  --user guest:guest \
  --header 'content-type: application/json' \
  --request PUT \
  --data '{"type":"topic","durable":true,"auto_delete":false,"internal":false,"arguments":{}}' \
  "http://localhost:${RABBITMQ_HTTP_PORT}/api/exchanges/%2f/unifiedfeed"

"${compose[@]}" exec --no-TTY pulsar bin/pulsar-admin sources create \
  --archive /pulsar/connectors/rabbitmq.nar \
  --name uof-rabbitmq \
  --destination-topic-name persistent://public/default/uof-feed \
  --source-config '{"host":"rabbitmq","port":5672,"virtualHost":"/","username":"guest","password":"guest","queueName":"uof-integration","exchangeName":"unifiedfeed","routingKey":"#","durable":true,"connectionName":"uof-integration"}'

for _ in {1..60}; do
  if "${compose[@]}" exec --no-TTY pulsar bin/pulsar-admin sources status \
    --name uof-rabbitmq | grep --quiet '"running"[[:space:]]*:[[:space:]]*true'; then
    (cd "${root_dir}" && "${mix_command[@]}" test --only integration)
    exit
  fi
  sleep 2
done

"${compose[@]}" logs pulsar
echo "RabbitMQ source did not reach the running state" >&2
exit 1
