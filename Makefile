.PHONY: build test lint vuln ci clean

PKG := ./...

build:
	go build $(PKG)

test:
	go test -race $(PKG)

lint:
	go vet $(PKG)
	golangci-lint run $(PKG)

vuln:
	govulncheck $(PKG)

ci: lint build test vuln

clean:
	go clean $(PKG)
	find . -name "cover*.out" -delete
	rm -f coverage.html
