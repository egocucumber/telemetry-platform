SHELL := /bin/sh
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: build test test-integration lint proto up down logs ps load bench tidy

build:
	@mkdir -p bin
	@for s in ingest processor alerter query simulator migrate; do \
		echo "building $$s"; CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/$$s ./cmd/$$s || exit 1; \
	done

test:
	go test -race -count=1 -cover ./...

test-integration:
	go test -race -count=1 -tags=integration -timeout=10m ./test/integration/...

bench:
	go test -run=^$$ -bench=. -benchmem ./internal/...

lint:
	golangci-lint run ./...
	buf lint

proto:
	buf generate

tidy:
	go mod tidy

up:
	$(COMPOSE) up -d --build

build-linux:
	@mkdir -p bin/linux
	@for s in ingest processor alerter query simulator migrate; do \
		echo "building $$s"; GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/linux/$$s ./cmd/$$s || exit 1; \
	done

up-local: build-linux
	DOCKERFILE=deploy/Dockerfile.local $(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f ingest processor alerter query simulator

ps:
	$(COMPOSE) ps

load:
	DEVICES=200 INTERVAL=500ms INCIDENT_RATE=0.001 ADMIN_ADDR=:9099 go run ./cmd/simulator
