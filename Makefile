SHELL := /bin/bash

# apache/pulsar-connectors#140 (messageKeyRules, __rabbitmq_routing_key).
# Any commit, branch or tag; the build is cached per ref.
RABBITMQ_SOURCE_REPO ?= https://github.com/apache/pulsar-connectors.git
RABBITMQ_SOURCE_REF ?= 7012976b702f0644c50494fb3988a2e7d5c5d65d
RABBITMQ_SOURCE_DIR ?= $(CURDIR)/tmp/pulsar-connectors-$(RABBITMQ_SOURCE_REF)
RABBITMQ_SOURCE_NAR ?= $(CURDIR)/tmp/rabbitmq-source-$(RABBITMQ_SOURCE_REF).nar
export RABBITMQ_SOURCE_NAR
MIX ?= $(shell command -v mix >/dev/null 2>&1 && echo mix || echo "mise exec -- mix")

.PHONY: deps compile test test-unit rabbitmq-source integration-up integration-down test-integration integration-test

deps:
	$(MIX) deps.get

compile: deps
	$(MIX) compile --warnings-as-errors

test: test-unit

test-unit: deps
	$(MIX) test

rabbitmq-source: $(RABBITMQ_SOURCE_NAR)

$(RABBITMQ_SOURCE_NAR):
	@set -euo pipefail; \
	if [[ ! -d "$(RABBITMQ_SOURCE_DIR)" ]]; then \
		git init --quiet "$(RABBITMQ_SOURCE_DIR)"; \
		git -C "$(RABBITMQ_SOURCE_DIR)" fetch --quiet --depth 1 \
			"$(RABBITMQ_SOURCE_REPO)" "$(RABBITMQ_SOURCE_REF)"; \
		git -C "$(RABBITMQ_SOURCE_DIR)" checkout --quiet FETCH_HEAD; \
	fi; \
	"$(RABBITMQ_SOURCE_DIR)/gradlew" -p "$(RABBITMQ_SOURCE_DIR)" :rabbitmq:assemble; \
	connector_nar="$$(find "$(RABBITMQ_SOURCE_DIR)/rabbitmq/build/libs" -maxdepth 1 \
		-name 'rabbitmq-*.nar' -print -quit)"; \
	if [[ -z "$$connector_nar" ]]; then \
		echo 'RabbitMQ connector NAR was not produced' >&2; \
		exit 1; \
	fi; \
	mkdir -p "$$(dirname "$(RABBITMQ_SOURCE_NAR)")"; \
	cp "$$connector_nar" "$(RABBITMQ_SOURCE_NAR)"

integration-up: rabbitmq-source
	@docker compose up --detach --wait || { docker compose down --volumes --remove-orphans; exit 1; }

integration-down:
	docker compose down --volumes --remove-orphans

test-integration: deps integration-up
	@trap 'docker compose down --volumes --remove-orphans' EXIT; $(MIX) test --only integration

integration-test: test-integration
