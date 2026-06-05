.PHONY: build test lint ci clean

PKG := ./...

build:
	go build $(PKG)

test:
	go test -race $(PKG)

lint:
	go vet $(PKG)
	golangci-lint run $(PKG)

ci: lint build test

clean:
	go clean $(PKG)
	find . -name "cover*.out" -delete
	rm -f coverage.html
