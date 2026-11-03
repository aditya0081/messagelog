# Every Go command runs inside Docker, so nothing is installed on the host.
# Run `make` to see all targets.

GO_RUN := docker run --rm -v $(CURDIR):/src -w /src \
	-v lld-gomod:/go/pkg/mod -v lld-gocache:/root/.cache/go-build golang:1.25
GO := $(GO_RUN) go

# The broker runs in a named container so the CLI can reach it over TCP.
NET    := messagelog-net
BROKER := messagelog-broker

.DEFAULT_GOAL := help
.PHONY: help build vet fmt test test-v broker broker-stop cli

help: ## show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## compile everything
	$(GO) build ./...

vet: ## go vet
	$(GO) vet ./...

fmt: ## gofmt all files in place
	$(GO_RUN) gofmt -w .

test: ## all tests with the race detector
	$(GO) test -race -count=1 ./...

test-v: ## all tests, verbose (filter with T=TestName, P=./internal/partition/...)
	$(GO) test -race -count=1 -v $(if $(P),$(P),./...) $(if $(T),-run $(T))

broker: ## start the broker in the background (data in ./data)
	-docker network create $(NET) >/dev/null 2>&1
	docker run -d --rm --name $(BROKER) --network $(NET) -v $(CURDIR):/src -w /src \
		-v lld-gomod:/go/pkg/mod -v lld-gocache:/root/.cache/go-build golang:1.25 \
		sh -c 'go build -o /tmp/broker ./cmd/broker && exec /tmp/broker -addr :9092 -data /src/data'
	# exec makes the broker PID 1 so `docker stop` (SIGTERM) reaches it and it shuts down cleanly.
	@echo "broker starting; logs: docker logs -f $(BROKER)"

broker-stop: ## stop the broker (SIGTERM, clean shutdown)
	docker stop $(BROKER)

cli: ## run the CLI against the broker, e.g. make cli ARGS="publish orders hello"
	docker run --rm --network $(NET) -v $(CURDIR):/src -w /src \
		-v lld-gomod:/go/pkg/mod -v lld-gocache:/root/.cache/go-build golang:1.25 \
		go run ./cmd/mlctl -addr $(BROKER):9092 $(ARGS)
