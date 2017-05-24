include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: test $(PKGS)
SHELL := /bin/bash
PKGS = $(shell go list ./...)
$(eval $(call golang-version-check,1.8))


export _DEPLOY_ENV=testing
export REDIS_TEST_URL ?= localhost:6379

test: $(PKGS)

$(PKGS): golang-test-all-strict-deps
	@go get -d -t $@
	$(call golang-test-all-strict,$@)
