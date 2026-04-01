.PHONY: fmt lint build test run coverage coverage-pkg coverage-func coverage-total

BINARY  := inboxatlas
CMD     := ./cmd/inboxatlas
SUMMARY_PROVIDER := ./cmd/openai-summary-provider
INFERENCE_PROVIDER := ./cmd/ai-inference-provider
GO ?= go
GO_TEST_ENV := env GOCACHE=/tmp/inboxatlas-gocache GOTMPDIR=/tmp/inboxatlas-gotmp
GO_TEST_PREP := mkdir -p /tmp/inboxatlas-gocache /tmp/inboxatlas-gotmp
GO_TEST_FLAGS :=

ifeq ($(OS),Windows_NT)
GO_TEST_FLAGS += -work
endif

# Format all Go source files.
fmt:
	$(GO) fmt ./...

# Run the golangci-lint linter suite.
lint: fmt
	golangci-lint run ./...

# Build the inboxatlas, ia, openai-summary-provider, and ai-inference-provider binaries.
build:
	$(GO) build -o inboxatlas $(CMD)
	$(GO) build -o ia $(CMD)
	$(GO) build -o openai-summary-provider.exe $(SUMMARY_PROVIDER)
	$(GO) build -o ai-inference-provider.exe $(INFERENCE_PROVIDER)

# Run all tests.
test:
	@$(GO_TEST_PREP)
	$(GO_TEST_ENV) $(GO) test $(GO_TEST_FLAGS) ./...

test-verbose:
	$(GO_TEST_ENV) $(GO) test ./... -v

# Generate coverage profile (prerequisite for all coverage-* targets).
coverage:
	@$(GO_TEST_PREP)
	$(GO_TEST_ENV) $(GO) test $(GO_TEST_FLAGS) -coverprofile=coverage.out ./...

# Function-level breakdown (includes total line at the bottom).
coverage-func: coverage
	@echo ""
	@echo "--- Function-level coverage ---"
	$(GO) tool cover -func=coverage.out

# Total repository coverage as a single summary line.
coverage-total: coverage
	@$(GO) tool cover -func=coverage.out | grep "^total:"

# Build and run the inboxatlas binary.
run: build
	./$(BINARY)
