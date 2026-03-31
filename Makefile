.PHONY: fmt lint build test run coverage coverage-pkg coverage-func coverage-total

BINARY  := inboxatlas
CMD     := ./cmd/inboxatlas
SUMMARY_PROVIDER := ./cmd/openai-summary-provider

# Format all Go source files.
fmt:
	go fmt ./...

# Run the golangci-lint linter suite.
lint: fmt
	golangci-lint run ./...

# Build the inboxatlas, ia, and openai-summary-provider binaries.
build:
	go build -o inboxatlas $(CMD)
	go build -o ia $(CMD)
	go build -o openai-summary-provider.exe $(SUMMARY_PROVIDER)

# Run all tests.
test:
	go test ./...

# Generate coverage profile (prerequisite for all coverage-* targets).
coverage:
	go test -coverprofile=coverage.out ./...

# Function-level breakdown (includes total line at the bottom).
coverage-func: coverage
	@echo ""
	@echo "--- Function-level coverage ---"
	go tool cover -func=coverage.out

# Total repository coverage as a single summary line.
coverage-total: coverage
	@go tool cover -func=coverage.out | grep "^total:"

# Build and run the inboxatlas binary.
run: build
	./$(BINARY)
