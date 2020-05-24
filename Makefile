SHELL := /bin/bash
PROJECT=acid_kvstore
GO_PATH ?= $(shell go env GOPATH)

# Ensure GO_PATH is set before running build process.
ifeq "$(GO_PATH)" ""
  $(error Please set the environment variable GOPATH before running `make`)
endif

GO                  := GO111MODULE=on go
GO_BUILD             := $(GO) build $(BUILD_FLAG) -tags codes
GO_TEST              := $(GO) test -v --count=1 --parallel=1 -p=1
TEST_LD_FLAGS        := ""
PACKAGE_LIST        := go list ./...| grep -vE "cmd"
PACKAGES            := $$($(PACKAGE_LIST))

CUR_DIR := $(shell pwd)
export PATH := $(CURDIR)/bin/:$(PATH)

proto:
	mkdir -p $(CURDIR)/bin
	(cd proto && ./generate_go.sh)
	GO111MODULE=on go build ./proto/pkg/...
