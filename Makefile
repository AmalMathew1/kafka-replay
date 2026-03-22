BINARY_NAME=kafka-replay
BUILD_DIR=.
GO=go
VERSION?=dev

.PHONY: build test test-cover test-unit test-integration lint fmt clean install

build:
	$(GO) build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/kafka-replay

test:
	$(GO) test ./... -v

test-unit:
	$(GO) test ./internal/... -v

test-integration:
	$(GO) test ./tests/... -v

test-cover:
	$(GO) test ./... -coverprofile=coverage.out
	$(GO) tool cover -func=coverage.out

lint:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

clean:
	rm -f $(BINARY_NAME) coverage.out

install:
	$(GO) install ./cmd/kafka-replay
