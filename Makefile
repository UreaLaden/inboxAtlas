.PHONY: fmt lint build test run coverage coverage-pkg coverage-func coverage-total

BINARY  := inboxatlas
CMD     := ./cmd/inboxatlas

# Format all Go source files.
fmt:
	go fmt ./...

# Run the golangci-lint linter suite.
lint: fmt
	golangci-lint run ./...

# Build both the inboxatlas and ia binaries.
build:
	go build -o inboxatlas $(CMD)
	go build -o ia $(CMD)

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
