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
export PATH := $(CUR_DIR)/bin/:$(PATH)

.PHONY: clean proto tx store replicamgr

all: proto tx store replicamgr


proto:
	(proto/generate_go.sh)
	GO111MODULE=on go build ./proto/package/...

tx:
	rm -rf tx/raftexample*
	$(GO_BUILD) -o tx/tx tx/main.go

store:
	rm -rf store/raftexample*
	$(GO_BUILD) -o store/store store/main.go
replicamgr:
	rm -rf replicaMgr/raftexample*
	$(GO_BUILD) -o replicaMgr/replicamgr replicaMgr/main.go
