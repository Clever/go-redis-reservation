SHELL := /bin/bash
.PHONY: $(PKG)

PKG := github.com/Clever/go-redis-reservation/reservation

REDIS_TEST_URL ?= localhost:6379

$(GOPATH)/bin/golint:
	@go get github.com/golang/lint/golint

$(PKG): $(GOPATH)/bin/golint
	go get -d -t $@
	gofmt -w=true $(GOPATH)/src/$@/*.go
	@echo ""
	@echo "LINTING $@..."
	$(GOPATH)/bin/golint $(GOPATH)/src/$@/*.go
	@echo "TESTING $@..."
	REDIS_TEST_URL=$(REDIS_TEST_URL) go test -v $@

test: $(PKG)
