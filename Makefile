.PHONY: test lint clean tools bench bench-mem

# Run tests and generate coverage report
test:
	go test -race -coverprofile=cover.out ./...
	go tool cover -html=cover.out -o cover.html

# Run linter
lint:
	golangci-lint run

# Clean generated files
clean:
	rm -f cover.out cover.html mem.out

# Install dependency tools
tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/k1LoW/octocov@latest
