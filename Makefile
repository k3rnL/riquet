SHELL := /bin/sh

GO ?= go
GOFLAGS ?=
BINARY := bin/riquet
VERSION ?= dev
COMMIT ?= unknown
BUILD_DATE ?= unknown
LDFLAGS := -s -w \
	-X github.com/k3rnL/riquet/internal/buildinfo.version=$(VERSION) \
	-X github.com/k3rnL/riquet/internal/buildinfo.commit=$(COMMIT) \
	-X github.com/k3rnL/riquet/internal/buildinfo.date=$(BUILD_DATE)

.PHONY: all build tools test test-race test-helm lint fmt fmt-check generate-check container clean

all: build

build:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/riquet

tools:
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -trimpath -o bin/riquet-backup ./cmd/riquet-backup
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -trimpath -o bin/riquet-export ./cmd/riquet-export
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -trimpath -o bin/riquet-restore ./cmd/riquet-restore

test:
	$(GO) test $(GOFLAGS) ./...

test-race:
	$(GO) test $(GOFLAGS) -race ./...

test-helm:
	$(GO) test $(GOFLAGS) ./test/helm -v

lint:
	golangci-lint run ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

fmt-check:
	test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))"

generate-check:
	$(GO) generate ./...
	git diff --exit-code

container:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE) -t riquet:$(VERSION) .

clean:
	rm -f $(BINARY)
