.PHONY: build test lint fmt tidy vuln examples ci clean

PKG := ./...

build:
	go build $(PKG)

test:
	go test -race $(PKG)

lint:
	go vet $(PKG)
	golangci-lint run $(PKG)

fmt:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "gofmt: the files above need formatting"; exit 1; }

examples:
	@for dir in examples/*/; do (cd "$$dir" && go vet ./... && go build ./...) || exit 1; done

tidy:
	@for dir in . examples/*/; do (cd "$$dir" && go mod tidy -diff) || exit 1; done

vuln:
	govulncheck $(PKG)

ci: lint fmt tidy build test examples vuln

clean:
	go clean $(PKG)
	find . -name "cover*.out" -delete
	rm -f coverage.html
