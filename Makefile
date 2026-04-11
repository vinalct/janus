COMPOSE_FILE := docker/docker-compose.yml
PODMAN_COMPOSE_FILE := docker/docker-compose.podman.yml
SERVICE := janus
ENVIRONMENT ?= local
JANUS_UID := $(shell id -u)
JANUS_GID := $(shell id -g)
JANUS_PROJECT_ROOT := $(CURDIR)

DETECT_COMPOSE = if podman compose version >/dev/null 2>&1; then echo 'podman compose'; elif command -v podman-compose >/dev/null 2>&1; then echo podman-compose; elif docker compose version >/dev/null 2>&1; then echo 'docker compose'; elif command -v docker-compose >/dev/null 2>&1; then echo docker-compose; else exit 1; fi

define RUN_COMPOSE
	@compose_cmd="$$( $(DETECT_COMPOSE) )" || { \
		echo "No compose-capable container engine found. Install Docker Compose, docker-compose, podman compose, or podman-compose." >&2; \
		exit 1; \
	}; \
	compose_files="$$(case "$$compose_cmd" in podman* ) printf '%s' '-f $(COMPOSE_FILE) -f $(PODMAN_COMPOSE_FILE)' ;; * ) printf '%s' '-f $(COMPOSE_FILE)' ;; esac)"; \
	container_user="$$(case "$$compose_cmd" in podman* ) printf '%s:%s' '$(JANUS_UID)' '$(JANUS_GID)' ;; * ) printf '%s:%s' '$(JANUS_UID)' '$(JANUS_GID)' ;; esac)"; \
	JANUS_CONTAINER_USER=$$container_user JANUS_UID=$(JANUS_UID) JANUS_GID=$(JANUS_GID) JANUS_PROJECT_ROOT=$(JANUS_PROJECT_ROOT) $$compose_cmd $$compose_files $(1)
endef

.PHONY: bootstrap check-compose up down status logs shell lint test run-local run-local-config docker-build docker-run clean

check-compose:
	@compose_cmd="$$( $(DETECT_COMPOSE) )" || { \
		echo "No compose-capable container engine found. Install Docker Compose, docker-compose, podman compose, or podman-compose." >&2; \
		exit 1; \
	}; \
	printf 'Using %s\n' "$$compose_cmd"; \
	case "$$compose_cmd" in podman* ) printf 'Using Podman keep-id user namespace for writable bind mounts\n' ;; esac

bootstrap: check-compose
	$(call RUN_COMPOSE,build $(SERVICE))

up: check-compose
	$(call RUN_COMPOSE,up -d --force-recreate $(SERVICE))

down: check-compose
	$(call RUN_COMPOSE,down)

status: check-compose
	$(call RUN_COMPOSE,ps)

logs: check-compose
	$(call RUN_COMPOSE,logs $(SERVICE))

shell: up
	$(call RUN_COMPOSE,exec $(SERVICE) sh)

lint: up
	$(call RUN_COMPOSE,exec -T $(SERVICE) python -m ruff check src tests)

test: up
	$(call RUN_COMPOSE,exec -T $(SERVICE) python -m pytest)

run-local: up
	$(call RUN_COMPOSE,exec -T $(SERVICE) python -m janus.main --environment $(ENVIRONMENT) --with-spark)

run-local-config: up
	$(call RUN_COMPOSE,exec -T $(SERVICE) python -m janus.main --environment $(ENVIRONMENT))

docker-build: bootstrap

docker-run: run-local

clean:
	rm -rf .pytest_cache .ruff_cache
