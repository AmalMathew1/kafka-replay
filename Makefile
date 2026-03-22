BINARY_NAME=kafka-replay
BUILD_DIR=.
GO=go

.PHONY: build test test-cover lint fmt clean install

build:
	$(GO) build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/kafka-replay

test:
	$(GO) test ./... -v

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
